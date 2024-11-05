-- GENERATE MAPPING TABLES
-- runtime: approx 30 mins

-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;



CREATE OR REPLACE PROCEDURE udw_clientsolutions_cs.sp_update_custom_creative_mapping(
    sales_order_source_table        VARCHAR     -- full table name for operative one sales data (db.schema.table)
    ,cdw_exposure_source_table      VARCHAR     -- full table name for CDW exposure data (db.schema.table)
    ,destination_table              VARCHAR     -- full table name to write map to (db.schema.table)
    ,s3_stage_path                  VARCHAR     -- file to write map to for CDW (@stage/path/file.csv)
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE 
    s3_stage_path_ts    VARCHAR;
    current_date        TIMESTAMP; 

BEGIN

    -- get current date
    current_date := CURRENT_DATE();

    -- set dates; objective: pull reporting for past -x days or past quarter, whichever is earlier
    -- replace '<date>' with timestamp (YYYYMMDD) in s3 file name (e.g., path/mapping/20240826_paramount_mapping.csv).
    -- script updated to go back 6 months instead of x days
    -- LET reporting_start     := (SELECT DATEADD('day', -49, CURRENT_DATE)::TIMESTAMP);
    LET reporting_start     := (SELECT DATEADD('month', -6, CURRENT_DATE)::TIMESTAMP);
    LET reporting_end       := (SELECT (DATEADD('day', -1, CURRENT_DATE)::VARCHAR || ' 23:59:59')::TIMESTAMP);
    LET quarter_start       := (SELECT DATE_TRUNC('quarter', CURRENT_DATE)::TIMESTAMP);

    s3_stage_path_ts        := (SELECT REPLACE(:s3_stage_path, '<date>', TO_CHAR(CURRENT_TIMESTAMP, 'yyyymmdd')));



    -- get creative data
    DROP TABLE IF EXISTS creative_name_info;
    CREATE TEMP TABLE creative_name_info AS (
        SELECT DISTINCT
            c.id AS creative_id
            ,c.name 
        FROM trader.creatives_latest c
    );



    -- get campaign data for active campaigns
    DROP TABLE IF EXISTS cmpgn;
    CREATE TEMP TABLE cmpgn AS (
        SELECT DISTINCT
            oms_att.sales_order_id
            ,cmpgn.id AS campaign_id
            ,cmpgn.name AS campaign_name
            ,oms_att.package_sales_order_line_item_id
        FROM trader.campaigns_latest AS cmpgn
            JOIN (
                SELECT DISTINCT
                    cmpgn_att.campaign_id
                    ,cmpgn_att.io_external_id AS sales_order_id
                    ,cmpgn_att.li_external_id AS package_sales_order_line_item_id
                FROM trader.campaign_oms_attrs_latest AS cmpgn_att
            ) AS oms_att ON cmpgn.id = oms_att.campaign_id
        WHERE 
            cmpgn.state != 'archived'
    );



    -- get exposure data
    DROP TABLE IF EXISTS cd;
    CREATE TEMP TABLE cd AS (
        SELECT
            p.vao
            ,ld.country
            ,GET(ld.samsung_tvids_pii_virtual_id, 0) AS vtifa
            ,ld.campaign_id
            ,ld.flight_id
            ,ld.creative_id
        FROM trader.log_delivery_raw_without_pii ld
            JOIN cmpgn c ON c.campaign_id = ld.campaign_id
            JOIN TABLE(:sales_order_source_table) p ON p.sales_order_id = c.sales_order_id
        WHERE 
            ld.event IN (
                'impression'    -- 1 impression
                ,'click'        -- 2 click
                ,'tracker'      -- 7 web
            )
            AND ld.country IS NOT NULL
            AND (ld.dropped != TRUE OR ld.dropped IS NULL)
            AND ld.udw_partition_datetime >= LEAST(:reporting_start, :quarter_start)
            AND ld.udw_partition_datetime <= :reporting_end
    );



    -- get date/time
    LET last_update_ts  := (SELECT CURRENT_TIMESTAMP);


    -- compose mapping data
    DROP TABLE IF EXISTS creative_map;
    CREATE TEMP TABLE creative_map AS (

        -- North America
        WITH na_cte AS (
            SELECT DISTINCT
                so.advertiser_name
                ,so.product_country_targeting
                ,cd.country
                ,CASE 
                    WHEN so.product_country_targeting = 'USA'
                    THEN 'US'
                    ELSE r.country_code_iso_3166_alpha_2
                END AS product_country_code_targeting
                ,COALESCE(r.region, '') AS region
                ,so.vao
                ,c.campaign_id
                ,c.campaign_name 
                ,cd.flight_id
                ,f.name AS flight_name
                ,f.start_at_datetime AS flight_start_date
                ,f.end_at_datetime AS flight_end_date
                ,so.package_sales_order_line_item_id AS line_item_id
                ,so.package_sales_order_line_item_name AS line_item_name
                ,cn.name AS creative_name 
                ,cn.creative_id
                ,so.package_sales_order_line_item_start_at AS line_item_start_ts
                ,so.package_sales_order_line_item_end_at AS line_item_end_ts
                ,so.advertiser_id AS advertiser_id
                ,so.sales_order_name AS insertion_order_name
                ,so.order_start_date AS campaign_start_date
                ,so.order_end_date AS campaign_end_date
                ,so.package_cost_type AS rate_type
                ,so.package_net_unit_cost AS rate
                ,CASE 
                    WHEN so.package_is_added_value = 1 
                    THEN so.package_added_value_amount 
                    ELSE so.package_net_cost 
                END AS booked_budget
                ,COALESCE(so.package_production_quantity, 0) AS placement_impressions_booked
                ,0 AS budget_delivered
                ,:last_update_ts AS last_update_ts
            FROM TABLE(:sales_order_source_table) so
                LEFT JOIN cmpgn c ON c.package_sales_order_line_item_id = so.package_sales_order_line_item_id
                LEFT JOIN cd ON cd.vao = so.vao 
                    AND cd.campaign_id = c.campaign_id
                LEFT JOIN creative_name_info cn ON cn.creative_id = cd.creative_id
                LEFT JOIN udw_lib.country_region_mapping_v r ON r.country_name = so.product_country_targeting
                LEFT JOIN trader.flights_latest f ON f.id = cd.flight_id
                    AND f.campaign_id = cd.campaign_id
            WHERE 
                1 = 1
                AND so.product_country_targeting IN ('USA', 'Canada', 'Mexico')
                AND so.vao IS NOT NULL
                AND so.sales_order_name IS NOT NULL
                AND so.sales_order_name != ''
                AND c.campaign_ID IS NOT NULL
                AND line_item_end_ts >= LEAST(:reporting_start, :quarter_start)
                AND line_item_start_ts <= :reporting_end
        )

        -- Global
        ,global_cte AS (
            SELECT DISTINCT
                so.advertiser_name
                ,so.product_country_targeting
                ,e.country
                ,CASE 
                    WHEN so.product_country_targeting = 'USA'
                    THEN 'US'
                    ELSE r.country_code_iso_3166_alpha_2
                END AS product_country_code_targeting
                ,COALESCE(r.region, '') AS region
                ,so.vao
                ,c.campaign_id
                ,c.campaign_name 
                ,e.flight_id AS flight_id
                ,f.name AS flight_name
                ,f.start_at_datetime AS flight_start_date
                ,f.end_at_datetime AS flight_end_date
                ,so.package_sales_order_line_item_id AS line_item_id
                ,so.package_sales_order_line_item_name AS line_item_name
                ,cn.name AS creative_name 
                ,e.creative_id AS creative_id
                ,so.package_sales_order_line_item_start_at AS line_item_start_ts
                ,so.package_sales_order_line_item_end_at AS line_item_end_ts
                ,so.advertiser_id AS advertiser_id
                ,so.sales_order_name AS insertion_order_name
                ,so.order_start_date AS campaign_start_date
                ,so.order_end_date AS campaign_end_date
                ,so.package_cost_type AS rate_type
                ,so.package_net_unit_cost AS rate
                ,so.package_net_cost AS booked_budget
                ,so.package_production_quantity AS placement_impressions_booked
                ,COALESCE(so.package_net_cost, so.package_added_value_amount, 0) AS budget_delivered
                ,:last_update_ts AS last_update_ts
            FROM TABLE(:sales_order_source_table) so
                LEFT JOIN cmpgn c ON c.package_sales_order_line_item_id = so.package_sales_order_line_item_id
                LEFT JOIN TABLE(:cdw_exposure_source_table) e ON e.vao = so.vao
                    AND e.campaign_id = c.campaign_id
                    AND e.exposure_datetime BETWEEN line_item_start_ts AND line_item_end_ts
                LEFT JOIN creative_name_info cn ON cn.creative_id = e.creative_id
                LEFT JOIN udw_lib.country_region_mapping_v r ON r.country_name = so.product_country_targeting
                LEFT JOIN trader.flights_latest f ON f.id = e.flight_id
                    AND f.campaign_id = e.campaign_id
            WHERE 
                1 = 1
                AND so.product_country_targeting NOT IN ('USA', 'Canada', 'Mexico')
                AND so.vao IS NOT NULL
                AND so.sales_order_name IS NOT NULL
                AND so.sales_order_name != ''
                AND c.campaign_ID IS NOT NULL
                AND line_item_end_ts >= LEAST(:reporting_start, :quarter_start)
                AND line_item_start_ts <= :reporting_end
        )

        -- join regions
        SELECT * FROM na_cte 
        UNION 
        SELECT * FROM global_cte 
    );

    -- SELECT * FROM creative_map LIMIT 1000;



    -- Remove all old data in the table
    LET stmt1 VARCHAR := 'DELETE FROM ' || :destination_table;
    EXECUTE IMMEDIATE stmt1;



    -- Insert new data
    LET stmt2 VARCHAR := 'INSERT INTO ' || :destination_table || '(SELECT * FROM creative_map)';
    EXECUTE IMMEDIATE stmt2;



    -- dump mapping files to data share for EU, SA, AP2 
    -- e.g., s3_stage_path = @udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/mapping/p_map.csv
    LET stmt3 VARCHAR := 'COPY INTO ' || s3_stage_path_ts || '
    FROM creative_map
    FILE_FORMAT     = (
        format_name                     = adbiz_data.mycsvformat999
        compression                     = ''none''
        NULL_IF                         = ()
        field_optionally_enclosed_by    = ''"''
    )
    SINGLE          = TRUE
    HEADER          = TRUE
    MAX_FILE_SIZE   = 4900000000
    OVERWRITE       = TRUE;
    ';

    EXECUTE IMMEDIATE stmt3;


    
    RETURN 'SUCCESS';


-- handle exception
EXCEPTION
    WHEN OTHER THEN
        SELECT udw_clientsolutions_cs.udf_submit_slack_notification_simple(
            slack_webhook_url => 'https://hooks.slack.com/triggers/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'
            ,date_string => :current_date::VARCHAR
            ,name_string => 'Snowflake Task Monitor'
            ,message_string => 'Procedure "udw_clientsolutions_cs.sp_update_custom_creative_mapping" failed.' || 
                ' Error: (' || :SQLCODE || ', ' || :SQLERRM || ')'
        );

        RETURN 'FAILED WITH ERROR(' || :SQLCODE || ', ' || :SQLERRM || ')';

END;
$$;

