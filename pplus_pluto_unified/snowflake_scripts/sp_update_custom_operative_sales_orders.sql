-- GET SALES ORDER DATA FROM OPERATIVE 1
-- Based on: https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/MnE/Enterprise/Disney/Q1'24/Weekly%20Report/DISNEY_OPERATIVE_SALES_ORDER.sql


-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;



CREATE OR REPLACE PROCEDURE udw_prod.udw_clientsolutions_cs.sp_update_custom_operative_sales_orders(advertiser_ids VARCHAR, destination_table VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN



    -- split id list to table
    DROP TABLE IF EXISTS advertiser_temp;
    CREATE TEMP TABLE advertiser_temp AS (
        SELECT 
            CAST(t.value AS INT) AS advertiser_id 
        FROM TABLE(SPLIT_TO_TABLE(:advertiser_ids, ',')) AS t
    );

    -- SELECT * FROM advertiser_temp;



    -- get sales data from operative 1
    DROP TABLE IF EXISTS sales_order;
    CREATE TEMP TABLE sales_order AS (
        SELECT
            q.* 
        FROM (
            SELECT
                so.sales_order_id
                ,so.sales_order_name
                ,so.order_start_date
                ,so.order_end_date
                ,so.external_opportunity_id
                ,so.advertiser_id
                ,so.advertiser_name
                ,so.agency_id
                ,so.agency_name
                ,so.primary_salesperson_id
                ,so.primary_salesperson_name
                ,so.owner_name
                ,so.order_primary_team_name
                ,so.sales_stage_id
                ,so.sales_stage_name
                ,so.order_status
                ,so.sales_order_version
                ,so.billing_account_id
                ,so.billing_account_name
                ,so.created_on
                ,so.last_modified_on
                ,so.net_order_cost
                ,so.order_currency_id
                ,so.time_zone
                ,ROW_NUMBER() OVER(PARTITION BY so.sales_order_id ORDER BY so.last_modified_on DESC, so.udw_partition_datetime DESC) AS row_num
            FROM operativeone.sales_order so
            WHERE 
                so.advertiser_id IN (
                    SELECT DISTINCT a.advertiser_id 
                    FROM advertiser_temp a
                )
        ) q
        WHERE q.row_num = 1
    );

    -- SELECT * FROM sales_orders_temp LIMIT 1000;



    -- get vao numbers for order ids from operative 1
    DROP TABLE IF EXISTS sales_order_custom_field;
    CREATE TEMP TABLE sales_order_custom_field AS (
        SELECT
            *
        FROM (
            SELECT
                so_custom.custom_field_value   --> vao
                ,so_custom.sales_order_id
                ,ROW_NUMBER() OVER(PARTITION BY so_custom.custom_field_value ORDER BY so_custom.last_modified_on DESC, so_custom.udw_partition_datetime DESC) AS row_num
            FROM operativeone.sales_order_custom_field so_custom
            JOIN sales_order so ON so.sales_order_id = so_custom.sales_order_id
            WHERE 
                so_custom.custom_field_name = 'JIRA ID'
                AND so_custom.custom_field_status = 'active'
                AND so_custom.custom_field_value IS NOT NULL
        )
        WHERE row_num = 1
    );

    -- SELECT * FROM sales_order_custom_field LIMIT 1000;



    -- get line items from operative 1
    DROP TABLE IF EXISTS sales_order_line_items;
    CREATE TEMP TABLE sales_order_line_items AS (
        SELECT
            q.*
        FROM (
            SELECT
                sol.sales_order_id
                ,sol.sales_order_line_item_id
                ,sol.sales_order_line_item_name
                ,sol.product_name
                ,sol.forecast_category
                ,TIMESTAMP_NTZ_FROM_PARTS(sol.sales_order_line_item_start_date::date, sol.start_time::time) AS sales_order_line_item_start_at
                ,TIMESTAMP_NTZ_FROM_PARTS(sol.sales_order_line_item_end_date::date, sol.end_time::time) AS sales_order_line_item_end_at
                ,sol.cost_type
                ,sol.quantity
                ,sol.production_quantity
                ,sol.unit_type
                ,sol.is_makegood
                ,sol.is_added_value
                ,sol.added_value_amount
                ,sol.net_cost
                ,sol.net_unit_cost
                ,sol.line_item_status
                ,sol.created_on
                ,sol.last_modified_on
                ,sol.parent_line_item_id
                ,sol.is_default_media_plan
                ,sol.media_plan_id
                ,sol.media_plan_name
                ,sol.section_id
                ,sol.section_name
                ,sol.product_id
                ,sol.package_id
                ,ROW_NUMBER() OVER(PARTITION BY sol.sales_order_line_item_id ORDER BY sol.last_modified_on DESC, sol.udw_partition_datetime DESC) AS row_num
            FROM operativeone.sales_order_line_items sol
                JOIN sales_order so ON so.sales_order_id = sol.sales_order_id
            WHERE 
                line_item_status NOT IN (
                    'deleted'
                    ,'In Sales Module Only'
                )
        ) q
        WHERE q.row_num = 1
    );

    -- SELECT * FROM sales_order_line_items LIMIT 1000;



    -- get product type (spec name) from operative 1 
    -- (e.g., First Screen Expandable Ad Tile, Carousel Static, TV Plus)
    DROP TABLE IF EXISTS products;
    CREATE TEMP TABLE products AS (
        SELECT
            *
        FROM (
            SELECT
                p.creative_spec_name
                ,p.product_id
                ,ROW_NUMBER() OVER(PARTITION BY p.product_id ORDER BY p.last_modified_date DESC, p.udw_partition_datetime DESC) AS row_num
            FROM operativeone.products p
            WHERE 
                p.allowed_booking_types != 'Not Bookable'
        ) q
        WHERE q.row_num = 1
    );

    -- SELECT * FROM products LIMIT 1000;



    -- get country and platform from operativbe 1
    DROP TABLE IF EXISTS product_custom_field;
    CREATE TEMP TABLE product_custom_field AS (
        SELECT
            *
        FROM (
            SELECT
                p.product_id
                ,CASE
                    WHEN p.custom_field_value = 'O&amp;O' THEN 'O&O'
                    WHEN p.custom_field_value = 'Korea' THEN 'South Korea'  --> As of 2023-10-16, we identify inconsistent country name for KR. Here we manually replace 'Korea' with 'South Korea'.
                    WHEN (p.custom_field_value = 'None' OR p.custom_field_value IS NULL) THEN 'United States'
                    ELSE p.custom_field_value
                END AS custom_field_value
                ,p.custom_field_id
                ,ROW_NUMBER() OVER(PARTITION BY p.product_id ORDER BY p.last_modified_on DESC, p.udw_partition_datetime DESC) AS row_num
            FROM operativeone.product_custom_field p
            WHERE 
                p.custom_field_id IN (
                    72      --> Country: France, Spain, United States, etc
                    ,28     --> platform: O&O, CTV, etc
                )
        ) q
        WHERE q.row_num = 1
    );

    -- SELECT * FROM product_custom_field LIMIT 1000;



    -- get date/time
    LET last_update_ts  := (SELECT CURRENT_TIMESTAMP);


    
    -- FINAL SELECTION
    DROP TABLE IF EXISTS output_table;
    CREATE TEMP TABLE output_table AS (
        SELECT
            so.sales_order_id
            ,TRIM(COALESCE(COALESCE(SPLIT_PART(so.sales_order_name,'|',2), SPLIT_PART(so.sales_order_name,'|',1)), so.sales_order_name)) AS sales_order_name
            ,so.order_start_date
            ,so.order_end_date
            ,TRY_TO_NUMBER(replace(so_jira_id.custom_field_value, 'VAO-', '')) AS vao
            ,so.advertiser_id
            ,so.advertiser_name
            ,so.sales_stage_name
            ,so.order_status                                                                            AS sales_order_status
            ,CASE 
                WHEN product_country.custom_field_value = 'United States' 
                THEN 'USA' 
                ELSE product_country.custom_field_value 
            END                                                                                         AS product_country_targeting
            ,so_li.line_item_status                                                                     AS sales_order_line_item_status
            ,COALESCE(so_li.parent_line_item_id, so_li.sales_order_line_item_id)                        AS package_sales_order_line_item_id
            ,COALESCE(package.sales_order_line_item_name, so_li.sales_order_line_item_name)             AS package_sales_order_line_item_name
            ,COALESCE(package.product_name, so_li.product_name)                                         AS package_product_name
            ,COALESCE(package_products.creative_spec_name, so_li_products.creative_spec_name)           AS package_creative_spec_name
            ,COALESCE(package.is_added_value, so_li.is_added_value)                                     AS package_is_added_value
            ,COALESCE(package.added_value_amount, so_li.added_value_amount)                             AS package_added_value_amount
            ,COALESCE(package.cost_type, so_li.cost_type)                                               AS package_cost_type
            ,COALESCE(package.net_unit_cost, so_li.net_unit_cost)                                       AS package_net_unit_cost
            ,COALESCE(package.net_cost, so_li.net_cost)                                                 AS package_net_cost
            ,COALESCE(package.unit_type, so_li.unit_type)                                               AS package_unit_type
            ,COALESCE(package.quantity, so_li.quantity)                                                 AS package_quantity
            ,COALESCE(package.production_quantity, so_li.production_quantity)                           AS package_production_quantity
            ,COALESCE(package.sales_order_line_item_start_at, so_li.sales_order_line_item_start_at)     AS package_sales_order_line_item_start_at
            ,COALESCE(package.sales_order_line_item_end_at, so_li.sales_order_line_item_end_at)         AS package_sales_order_line_item_end_at
            ,:last_update_ts AS last_update_ts
        FROM sales_order                            so
            LEFT JOIN sales_order_custom_field      so_jira_id          ON so.sales_order_id = so_jira_id.sales_order_id                                                        --> VAO
            LEFT JOIN sales_order_line_items        so_li               ON so.sales_order_id = so_li.sales_order_id                                                             --> line item
            LEFT JOIN sales_order_line_items        package             ON so_li.parent_line_item_id = package.sales_order_line_item_id                                         --> parent line item
            LEFT JOIN products                      so_li_products      ON so_li.product_id = so_li_products.product_id                                                         --> line item product 
            LEFT JOIN products                      package_products    ON package.product_id = package_products.product_id                                                     --> parent line item product
            LEFT JOIN product_custom_field          product_country     ON so_li_products.product_id = product_country.product_id AND product_country.custom_field_id = 72      --> custom country
            LEFT JOIN product_custom_field          product_platform    ON so_li_products.product_id = product_platform.product_id AND product_platform.custom_field_id = 18    --> custom platform
        WHERE 
            sales_order_status NOT IN ('closed_lost', 'deleted')
            AND so.sales_stage_name NOT IN ('Closed Lost')
            AND (
                (so.order_start_date >= DATEADD('MONTH', -18, CURRENT_DATE) OR so.order_start_date IS NULL)                                             --> order start date in last 18 months or is NULL
                OR (so.order_end_date >= DATEADD('MONTH', -18, CURRENT_DATE) OR so.order_end_date IS NULL)                                              --> order end date in last 18 months or is NULL
                OR (package_sales_order_line_item_start_at >= DATEADD('MONTH', -18, CURRENT_DATE) OR package_sales_order_line_item_start_at IS NULL)    --> line start date in last 18 months or is NULL
                OR (package_sales_order_line_item_end_at >= DATEADD('MONTH', -18, CURRENT_DATE) OR package_sales_order_line_item_end_at IS NULL)        --> line start date in last 18 months or is NULL
            )
        ORDER BY 
            so.sales_order_id DESC
            ,so.order_start_date ASC
            ,package_sales_order_line_item_start_at ASC
            ,so.order_end_date ASC
            ,package_sales_order_line_item_end_at ASC
    );

    -- SELECT * FROM output_table LIMIT 1000;



    -- Remove all old data in the table
    LET stmt1 VARCHAR := 'DELETE FROM ' || :destination_table;
    EXECUTE IMMEDIATE stmt1;

    -- Insert new data
    LET stmt2 VARCHAR := 'INSERT INTO ' || :destination_table || '(SELECT * FROM output_table)';
    EXECUTE IMMEDIATE stmt2;


    
    RETURN 'SUCCESS';

END;
$$;

