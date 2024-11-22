/**************************************************

Description: 
    Hulu ad-hoc reports for liveramp subscribers. These reports are non-sequential  due to the fact that 
    the liveramp Hulu subscriber psid list doesn't contain any timestamps so we don't know exactly when they subscribed.
    As a result of this, we just look for overlap between the subscriber list and samsung universe ("matches" or "matched users") 
    and provide a lift report on app usage and time spent for "matches".

Client:         Hulu
Parent Report:  N/A
Created by:     Vaughn Haynes

Methodologies:
Measurement KPIS = App Opens, Time Spent
Measurement Period = Campaign duration
Attribution Window = N/A
Last/Any Touch = N/A
Time Sequential = No
Minimum App Usage Duration = 1 min s
Lift Analysis = Exposed vs Unexposed, Frequency, Placement, Creative, Flight, Daily
Lookback Window = N/A 

Markets: US

Data Sources: UDW

Data Schemas:
    - udw_clientsolutions_cs, udw_lib, salesforce, operativeone, trader, 
      trader, profile_tv, data_tv_smarthub, adbiz_data

 Template: Hulu - LiveRamp & App Pixel Attribution Report
 Wiki: https://adgear.atlassian.net/wiki/spaces/MAST/pages/19409273050/Hulu+-+LiveRamp+App+Pixel+Attribution+Report

 Ticket:
    https://adgear.atlassian.net/browse/SAI-6738

 Original query:
    https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/MnE/Content_Partner/Hulu/US_UDW/New_Attribution_Report_Template/202407_hulu_liveramp/hulu_liveramp_6441.sql


 Instructions:
 -- Follow setup steps 1 - 3 below.
 - Update vao_list variable to include 1 or more vaos from the JIRA ticket (e.g. '126354, 938726')
 - Update the stage file path in temp table "liveramp_subscribers_s3" with the path from the ticket (e.g., 
        CONVERT: s3://samsung-dm-data-share-analytics/export/20240508/psid/321480.csv
        TO: @udw_prod.udw_clientsolutions_cs.samsung_dm_data_share_analytics/export/20240508/psid/321480.csv
    ) OR use the "COPY FILES" logic below to move it to a custom directory since the default file location is deleted after a few days
 - Run the report (approx 70 mins)
 - Get template: https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/MnE/Content_Partner/Hulu/US_UDW/New_Attribution_Report_Template/
    Filename: 20240513_hulu_liveramp.xlsx
 - There will be multiple tabs from the query output. Start with tab 6 and copy the data to the Excel template (1 tab per table)
 - Be sure to update the Name and Flight Period metadata at the top of the excel file. This info is META tabs 4-5.
 - Be sure to update the VAO column on each tab from "combined" to use the correct VAO if only 1 VAO is used. Info on tab 4. 
 - When pasting data, if you see the error "This wont work because it would move cells in a table...", select the table below the one
    you are pasting to and drag it down to make room. Try re-pasting. If the tab;e you are adding to will expand and olverlap the table below it
    then you get that error so just make room by dragging it down.
- Rename the file "0000_Hulu_Attribution.xlsx" where 0000 is the Jira Ticket number

**************************************************/


SELECT 'STARTING QUERY. Remember to complete set-up steps below or next tab and report will be empty!!!' AS message;


-- SETUP
/**
---------------------------------
STEP 1. FIND AND REPLACE VALUES
---------------------------------
Find and eplace: 

    6738            --> Jira ticket number (https://adgear.atlassian.net/browse/SAI-6738)
    20241007        --> timestamp from liveramp subscriber file's destination path (from ticket) (s3://samsung-dm-data-share-analytics/export/20241007/psid/321480.csv)
    321480.csv      --> file name from liveramp subscriber file's destination path (from ticket)
    187896          --> vao number from ticket (VAO-187896)

---------------------------------
STEP 2. COPY FILES
---------------------------------
Run script below:

    COPY FILES
    INTO @udw_prod.udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/hulu/SAI6738/20241007/
    FROM @udw_prod.udw_clientsolutions_cs.samsung_dm_data_share_analytics/export/20241007/psid/
    FILES = ('321480.csv');

    LIST @udw_prod.udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/hulu/SAI6738/20241007/321480.csv

---------------------------------
STEP 3. CHECK VARIABLES BELOW (OPTIONAL)
---------------------------------
If you have mmore than one VAO, change variable to a list:
    '187896' => '187896, 12345, 67890'

If you need to override the default campaign start and end date then change NULL to dates:
    NULL => '2024-01-01'

Otherwise there is no need to update the variables if you performed step 1 above.
**/
  

-- START REPORT QUERY
-- verifies file was copied
 LIST @udw_prod.udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/hulu/SAI6738/20241007/;


-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- ===================================================
-- START SETTINGS
-- ===================================================

-- set variables 
-- change country, app name, vao
SET (
    reporting_country, 
    app_name,
    vao_list,
    attribution_window_unit,
    attribution_window_liveramp
) = (
    'US',               --> reporting_country
    'Hulu',             --> app_name
    '187896',           --> vao_list
    'DAY',              --> attribution_window_unit 
    0                   --> attribution_window_liveramp 
);

-- set override variables ONLY if needing to override report dates
-- otherwise, set to NULL
SET (
    override_report_start_date
    ,override_report_end_date
) = (
    NULL        --> override_report_start_date; format 'YYYY-MM-DD' OR NULL
    ,NULL       --> override_report_end_date; format 'YYYY-MM-DD' OR NULL
);

-- ===================================================
-- END SETTINGS
-- ===================================================


/**
Audience S3 Intake
---------------------
Import Hulu DMP audience from s3
    1. copy to location below
    2. import to temp table "liveramp_subscribers_s3"
**/
DROP TABLE IF EXISTS liveramp_subscribers_s3;
CREATE temp TABLE liveramp_subscribers_s3 (psid VARCHAR(512));
COPY INTO liveramp_subscribers_s3
FROM @udw_prod.udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/hulu/SAI6738/20241007/321480.csv
FILE_FORMAT = (format_name = adbiz_data.analytics_csv);


DROP TABLE IF EXISTS liveramp_subscribers;
CREATE TEMP TABLE liveramp_subscribers AS (

    SELECT DISTINCT
        vtifa
    FROM liveramp_subscribers_s3 a
        LEFT JOIN udw_lib.virtual_psid_tifa_mapping_v m ON LOWER(m.psid) = a.psid

);

SELECT COUNT(*) FROM liveramp_subscribers;


/*******************
 Convert lists to tables

 Allows a single variable to be used to specify one or moultiple values to be used in the query
*******************/
-- vao list to table
DROP TABLE IF EXISTS vaos_table;
CREATE TEMP TABLE vaos_table AS (

    SELECT CAST(t.value AS INT) AS vao
    FROM TABLE(SPLIT_TO_TABLE($vao_list, ',')) AS t

);

-- SELECT * FROM vaos_table;


/********************
 Campaign Mapping

 https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/Basic_Queries/Campaign_Mapping/VAO%20to%20Line%20Item%20and%20Campaign%2BFlight%2BCreative
********************/
DROP TABLE IF EXISTS campaign_meta;
CREATE TEMP TABLE campaign_meta AS (

    WITH vao_samsungCampaignID AS (
        SELECT
            vao,
            samsung_campaign_id,
            sales_order_id,
            sales_order_name
        FROM
            (
            SELECT
                CAST(replace(sf_opp.jira_id__c, 'VAO-', '') AS INT) AS vao,
                sf_opp.samsung_campaign_id__c AS samsung_campaign_id,
                sf_opp.operative_order_id__c AS sales_order_id,
                sf_opp.order_name__c AS sales_order_name,
                ROW_NUMBER() OVER(PARTITION BY vao ORDER BY sf_opp.lastmodifieddate DESC) AS rn
            FROM SALESFORCE.OPPORTUNITY AS sf_opp
            WHERE vao IN (SELECT DISTINCT vao FROM vaos_table)
            )
        WHERE rn = 1
    ),

    salesOrder AS (
        SELECT
            sales_order_id,
            sales_order_name,
            order_start_date,
            order_end_date,
            time_zone
        FROM
            (
            SELECT
                sales_order.sales_order_id,
                sales_order.sales_order_name,
                sales_order.order_start_date,
                sales_order.order_end_date,
                sales_order.time_zone,
                ROW_NUMBER() OVER(PARTITION BY sales_order.sales_order_id ORDER BY sales_order.last_modified_on DESC) AS rn
            FROM OPERATIVEONE.SALES_ORDER AS sales_order
            JOIN vao_samsungCampaignID AS vao
                USING (sales_order_id)
            ) AS foo
        WHERE foo.rn = 1
    ),

    cmpgn AS (
        SELECT DISTINCT
            sales_order_id,
            sales_order_line_item_id,
            cmpgn.id AS campaign_id,
            cmpgn.name AS campaign_name,
            flight_id, 
            creative_id,
            rate_type,
            net_unit_cost,
            cmpgn.start_at_datetime::TIMESTAMP AS cmpgn_start_datetime_utc,
            cmpgn.end_at_datetime::TIMESTAMP AS cmpgn_end_datetime_utc
        FROM TRADER.CAMPAIGNS_LATEST AS cmpgn
        JOIN
            (
            SELECT DISTINCT
                cmpgn_att.campaign_id,
                cmpgn_att.rate_type,
                cmpgn_att.net_unit_cost,
                cmpgn_att.io_external_id AS sales_order_id,
                cmpgn_att.li_external_id AS sales_order_line_item_id
            FROM TRADER.CAMPAIGN_OMS_ATTRS_LATEST AS cmpgn_att
            JOIN vao_samsungCampaignID
                ON vao_samsungCampaignID.sales_order_id = cmpgn_att.external_id
            ) AS foo
            ON cmpgn.id = foo.campaign_id
        JOIN 
            (
            SELECT DISTINCT 
                campaign_id,
                flight_id, 
                creative_id
            FROM UDW_PROD.UDW_CLIENTSOLUTIONS_CS.CAMPAIGN_FLIGHT_CREATIVE
            ) c
            ON cmpgn.id = c.campaign_id
    ),

    flight AS (
        SELECT DISTINCT
            cmpgn.sales_order_id,
            flight.id AS flight_id,
            flight.name AS flight_name,
            flight.start_at_datetime::TIMESTAMP AS flight_start_datetime_utc,
            flight.end_at_datetime::TIMESTAMP AS flight_end_datetime_utc
        FROM TRADER.FLIGHTS_LATEST AS flight
        JOIN cmpgn
            USING (campaign_id)
    ),

    creative AS (
        SELECT DISTINCT 
            cmpgn.sales_order_id,
            creative.id AS creative_id,
            creative.name AS creative_name
        FROM TRADER.CREATIVES_LATEST AS creative
        JOIN cmpgn
            ON cmpgn.creative_id = creative.id
    ),

    lineItem AS (
        SELECT
            sales_order_id,
            sales_order_line_item_id,
            sales_order_line_item_name,
            sales_order_line_item_start_datetime_utc,
            sales_order_line_item_end_datetime_utc
        FROM (
            SELECT
                lineItem.sales_order_id,
                lineItem.sales_order_line_item_id,
                lineItem.sales_order_line_item_name,
                TIMESTAMP_NTZ_FROM_PARTS(lineItem.sales_order_line_item_start_date::date, lineItem.start_time::time) AS sales_order_line_item_start_datetime_utc,
                TIMESTAMP_NTZ_FROM_PARTS(lineItem.sales_order_line_item_end_date::date, lineItem.end_time::time) AS sales_order_line_item_end_datetime_utc,
                ROW_NUMBER() OVER(PARTITION BY lineItem.sales_order_line_item_id ORDER BY lineItem.last_modified_on DESC) AS rn
            FROM OPERATIVEONE.SALES_ORDER_LINE_ITEMS AS lineItem
            JOIN vao_samsungCampaignID AS vao
                USING (sales_order_id)
        ) AS foo
        WHERE foo.rn = 1
    )

    /******************************************************************************************
    * Main query          *** Remember to edit the parts you want to keep in below as well!
    ******************************************************************************************/
    SELECT DISTINCT
        /******************************
        * VAO info
        ******************************/
        vao_samsungCampaignID.vao,
        vao_samsungCampaignID.samsung_campaign_id,
        vao_samsungCampaignID.sales_order_id,
        vao_samsungCampaignID.sales_order_name,
        /******************************
        * Sales Order info
        ******************************/
        salesOrder.order_start_date,
        salesOrder.order_end_date,
        /******************************
        * Campaign info
        ******************************/
        cmpgn.campaign_id,
        cmpgn.campaign_name,
        cmpgn.flight_id,
        cmpgn.creative_id,
        cmpgn.rate_type,
        cmpgn.net_unit_cost,
        cmpgn.cmpgn_start_datetime_utc,
        cmpgn.cmpgn_end_datetime_utc,
        /******************************
        * Flight info
        ******************************/
        flight.flight_name,
        flight.flight_start_datetime_utc,
        flight.flight_end_datetime_utc,
        /******************************
        * Creative info
        ******************************/
        creative.creative_name,
        /******************************
        * Line Item info
        ******************************/
        lineItem.sales_order_line_item_id,
        lineItem.sales_order_line_item_name,
        lineItem.sales_order_line_item_start_datetime_utc,
        lineItem.sales_order_line_item_end_datetime_utc
    FROM vao_samsungCampaignID
        JOIN salesOrder USING (sales_order_id)
        JOIN cmpgn USING (sales_order_id)
        JOIN flight USING (sales_order_id, flight_id)
        JOIN creative USING (sales_order_id, creative_id)
        JOIN lineItem USING (sales_order_id, sales_order_line_item_id)
    WHERE 
        1 = 1
        -- AND lineItem.sales_order_line_item_name LIKE '%OMITB%'   --> uncomment this line to filter report to specific lines (e.g., contains 'OMITB' in name)
        -- AND creative.creative_name LIKE '%OMITB%'                --> uncomment this line to filter report to specific creatives (e.g., contains 'OMITB' in name)
);

SELECT * FROM campaign_meta LIMIT 1000;


/**
campaign start date
-------------------
use earliest campaign start date unless override specified
**/
SET campaign_start = (
    SELECT 
        (
            CASE 
                WHEN $override_report_start_date IS NULL 
                THEN MIN(cmpgn_start_datetime_utc) 
                ELSE CAST($override_report_start_date  || ' 00:00:00' AS DATE) 
            END
        )::TIMESTAMP
    FROM campaign_meta
);


/*
campaign end date
-------------------
use latest campaign end date unless override specified
**/
SET campaign_end = (
    SELECT 
        (
            CASE 
                WHEN $override_report_end_date IS NULL 
                THEN MAX(cmpgn_end_datetime_utc) 
                ELSE CAST($override_report_end_date || ' 23:59:59' AS DATE) 
            END
        )::TIMESTAMP
    FROM campaign_meta
);


/********************
 Samsung Universe

 https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/Basic_Queries/Samsung%20Universe%20Update.sql
 
 Samsung Universe (aka. superset) is a collection of Samsung TVs that can be found in any of following 3 data sources:
    - TV Hardware: profile_tv.fact_psid_hardware_without_pii
    - App Open: data_tv_smarthub.fact_app_opened_event_without_pii
    - O&O Samsung Ads Campaign Delivery: trader.log_delivery_raw_without_pii (for exchange_id = 6 and exchange_seller_id = 86) 
 
 Any data used for attribution reports needs to be intersected with Samsung Universe
 Reference: https://adgear.atlassian.net/wiki/spaces/MAST/pages/19673186934/M+E+Analytics+-+A+I+Custom+Report+Methodology
********************/
-- qualifier: start date = start date + 30 days if device graph resolution mechanism is used
SET report_start_date = $campaign_start;
SET report_end_date = $campaign_end;
SET country = $reporting_country;

DROP TABLE IF EXISTS qualifier; 
CREATE TEMP TABLE qualifier AS (
	SELECT 
		LISTAGG(DISTINCT '"'||EXCHANGE_SELLER_ID||'"', ',') AS exchage_seller_id_list,
		CASE 
			WHEN NOT exchage_seller_id_list LIKE ANY ('%"86"%', '%"88"%', '%"1"%', '%"256027"%', '%"237147"%', '%"escg8-6k2bc"%', '%"amgyk-mxvjr"%' ) 
			THEN 'Superset +30 days'
			ELSE 'Superset' 
		END AS qualifier, 
		CASE 
			WHEN qualifier = 'Superset +30 days' 
			THEN DATEADD(DAY, -30, $REPORT_START_DATE)::TIMESTAMP 
			ELSE $REPORT_START_DATE 
		END AS report_start_date
	FROM trader.log_delivery_raw_without_pii a
		JOIN campaign_meta b ON a.campaign_id = b.campaign_id
	WHERE 
		a.UDW_PARTITION_DATETIME BETWEEN $REPORT_START_DATE AND $REPORT_END_DATE
		AND a.event = 'impression'
		AND a.country = $COUNTRY
        AND a.campaign_id IS NOT NULL
);

SET report_start_date_qual = (SELECT report_start_date FROM qualifier);

DROP TABLE IF EXISTS samsung_ue; --5 mins IN M
CREATE TEMP TABLE samsung_ue AS (
	SELECT DISTINCT m.vtifa
	FROM PROFILE_TV.FACT_PSID_HARDWARE_WITHOUT_PII a
		JOIN UDW_LIB.VIRTUAL_PSID_TIFA_MAPPING_V m ON a.PSID_PII_VIRTUAL_ID = m.vpsid
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN $REPORT_START_DATE_QUAL AND $REPORT_END_DATE
		AND partition_country = $COUNTRY	
	UNION
	SELECT DISTINCT GET(SAMSUNG_TVIDS_PII_VIRTUAL_ID , 0) AS vtifa
	FROM trader.log_delivery_raw_without_pii 	
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN $REPORT_START_DATE_QUAL AND $REPORT_END_DATE
		AND event = 'impression'
		AND (dropped != TRUE OR  dropped IS NULL)
		AND (EXCHANGE_ID = 6 OR EXCHANGE_SELLER_ID = 86)
		AND country = $COUNTRY
	UNION 
	SELECT DISTINCT m.vtifa
	FROM DATA_TV_SMARTHUB.FACT_APP_OPENED_EVENT_WITHOUT_PII a 
		JOIN UDW_LIB.VIRTUAL_PSID_TIFA_MAPPING_V m ON a.PSID_PII_VIRTUAL_ID = m.vpsid
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN $REPORT_START_DATE_QUAL AND $REPORT_END_DATE
		AND partition_country = $COUNTRY
);

-- SELECT COUNT(*) AS cnt FROM samsung_ue;  


/**
Impressions
--------------
get impression data for campaign
**/
DROP TABLE IF EXISTS cd;
CREATE TEMP TABLE cd AS (

    SELECT
        ld.country
        ,GET(ld.samsung_tvids_pii_virtual_id, 0) AS vtifa
        ,FLOOR(PARSE_JSON(ld.timestamp))::INT::TIMESTAMP_NTZ AS timing
        ,cm.vao
        ,cm.sales_order_line_item_id AS line_item_id
        ,cm.sales_order_line_item_name AS line_item_name
        ,cm.creative_id
        ,cm.creative_name
        ,cm.flight_id
        ,cm.flight_name
        ,COUNT(*) AS imps
    FROM trader.log_delivery_raw_without_pii ld
        JOIN campaign_meta AS cm USING (campaign_id, flight_id, creative_id)
    WHERE 
        ld.udw_partition_datetime BETWEEN $campaign_start AND DATEADD($attribution_window_unit, $attribution_window_liveramp, $campaign_end)
        AND ld.event IN ('impression')
        AND ld.country = $reporting_country
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

);


/**
App usage
------------
Get usage of 60 seconds or more for Hulu app from 
the start of the campaign to the end of the campaign + n days attribution window.
**/
DROP TABLE IF EXISTS app_usg;
CREATE TEMP TABLE app_usg AS (

    WITH fact AS (
        SELECT
            fact.country
            ,pii_map.vtifa
            ,fact.start_timestamp AS timing
            ,fact.start_timestamp
            ,fact.end_timestamp
        FROM data_tv_acr.fact_app_usage_session_without_pii AS fact
            JOIN adbiz_data.lup_app_cat_genre_2023 AS am USING (app_id)
            LEFT JOIN udw_lib.virtual_psid_tifa_mapping_v AS pii_map ON pii_map.vpsid = fact.psid_pii_virtual_id
        WHERE 
            DATEDIFF('second', fact.start_timestamp, fact.end_timestamp) >= 60
            AND udw_partition_datetime BETWEEN $campaign_start AND DATEADD($attribution_window_unit, $attribution_window_liveramp, $campaign_end)
            AND fact.country = $reporting_country
            AND am.prod_nm = $app_name
    )

    SELECT DISTINCT
        country
        ,vtifa
        ,timing  --> app usage start timestamp
        ,COUNT(*) AS app_opens
        ,SUM(DATEDIFF('hours', start_timestamp, end_timestamp)) AS hour_spent
    FROM fact
        JOIN samsung_ue USING (vtifa)
    WHERE 
        fact.country = $country
    GROUP BY 1,2,3

);


/**
hulu subscribers in universe
----------------------------------
get overlap or matches between subscribers and samsung universe. Goal is to
get a list of subscribers who from hulu who are also in samsun universe
**/
DROP TABLE IF EXISTS liveramp_matched_audience;
    CREATE TEMP TABLE liveramp_matched_audience AS (

    SELECT DISTINCT
        $country AS country
        ,l.vtifa
    FROM samsung_ue s
        JOIN liveramp_subscribers l USING (vtifa)

);


/**
hulu subscribers app usage
----------------------------
gets sum of app usage for hulu subscribers in universe.
non-sequential attribution used as we dont know when the user subscribed
**/
DROP TABLE IF EXISTS liveramp_matched_app_usg;
CREATE TEMP TABLE liveramp_matched_app_usg AS (

    SELECT DISTINCT
        CONCAT('Samsung matched ', $app_name, ' subscribed app users') AS auds_cate   -- ' app users' -> ' subscribed app users'
        ,country
        ,timing
        ,vtifa
        ,SUM(app_opens) AS app_opens_conv
        ,SUM(hour_spent) AS hour_spent_conv
    FROM app_usg
        JOIN liveramp_matched_audience USING (country, vtifa)
    GROUP BY 1, 2, 3, 4

);


/********************
 META
*************************************************************************************************************************************************************/
SHOW VARIABLES;


SELECT $reporting_country, $app_name, $vao_list, $campaign_start, $campaign_end;


-- Start Output
-- cache size in temp table to avoid calling same CTE for each output table
DROP TABLE IF EXISTS universe_size;
CREATE TEMP TABLE universe_size AS (

    SELECT
        $country AS country
        ,COUNT(DISTINCT s.vtifa) AS total_universe
    FROM samsung_ue s
    GROUP BY 1

);


/**
Summary
*************************************************************************************************************************************************************/
WITH subscriber_counts_by_audeince_cte AS ( 
    SELECT
        CONCAT('Samsung matched ', $app_name, ' subscribed app users') AS auds_cate
        ,COUNT(DISTINCT vtifa) AS cnt_liveramp_raw
    FROM liveramp_subscribers
)

,app_user_counts_by_audeince_cte AS (
    SELECT
        auds_cate
        ,COUNT(DISTINCT vtifa) AS cnt_liveramp_samsung_matched
    FROM liveramp_matched_app_usg
    GROUP BY 1
)

-- General Info
SELECT DISTINCT
    s.auds_cate                                                       
    ,s.cnt_liveramp_raw                     
    ,a.cnt_liveramp_samsung_matched 
FROM subscriber_counts_by_audeince_cte s
    JOIN app_user_counts_by_audeince_cte a USING (auds_cate)
;


/**
Overall Lift
*************************************************************************************************************************************************************/
WITH exposed_vtifa_cte AS ( 
    SELECT DISTINCT
        country
        ,vao
        ,vtifa
    FROM cd
)

,exposed_app_stats_cte AS (
    SELECT
        v.country
        ,l.auds_cate
        ,'overall' AS campaign_id
        ,'overall' AS campaign_name
        ,COUNT(DISTINCT v.vtifa) AS exposed_app_user_counts
        ,SUM(l.hour_spent_conv) AS exposed_time_spent_hours
    FROM liveramp_matched_app_usg l
        JOIN exposed_vtifa_cte v USING (country, vtifa)
    GROUP BY 1, 2, 3, 4
)

,impressions_cte AS (
    SELECT
        country
        ,vao
        ,'overall' AS campaign_id
        ,'overall' AS campaign_name
        ,COUNT(DISTINCT vtifa) AS reach
        ,SUM(imps) AS imps
    FROM cd
    GROUP BY 1, 2, 3, 4
)

,app_stats_cte AS (
    SELECT
        l.country
        ,COUNT(DISTINCT l.vtifa) AS total_app_user_counts
        ,SUM(l.hour_spent_conv) AS total_time_spent_hours
    FROM liveramp_matched_app_usg l
    GROUP BY 1
)

SELECT DISTINCT
    i.vao
    ,e.auds_cate
    ,i.campaign_name
    ,e.exposed_app_user_counts
    ,e.exposed_time_spent_hours
    ,i.reach
    ,i.imps
    ,CAST(i.imps AS FLOAT)/i.reach AS freq
    ,a.total_app_user_counts
    ,a.total_time_spent_hours
    ,u.total_universe
    ,u.total_universe - i.reach AS total_unexpd_uni_superset
    ,a.total_app_user_counts - e.exposed_app_user_counts AS unexposed_app_user_counts
    ,a.total_time_spent_hours - e.exposed_time_spent_hours AS unexposed_time_spent_hours
    ,CAST(e.exposed_app_user_counts AS FLOAT) / i.reach AS expd_conv_rate
    ,CAST(unexposed_app_user_counts AS FLOAT) / total_unexpd_uni_superset AS unexpd_conv_rate
    ,CAST(unexposed_time_spent_hours AS FLOAT) / unexposed_app_user_counts AS avg_unexpd_hour_spent
    ,CAST(e.exposed_time_spent_hours AS FLOAT) / e.exposed_app_user_counts AS avg_expd_hour_spent
    ,expd_conv_rate / unexpd_conv_rate - 1  AS lift
    ,avg_expd_hour_spent / avg_unexpd_hour_spent - 1 AS lift_time_spent
FROM exposed_app_stats_cte e
    JOIN impressions_cte i USING (country, campaign_id, campaign_name)
    JOIN app_stats_cte a USING (country)
    JOIN universe_size u USING (country)
;


/**
Lift by Frequency
*************************************************************************************************************************************************************/
WITH exposed_vtifa_cte AS (
    SELECT
        country
        ,vao
        ,vtifa
        ,SUM(imps) AS imps
    FROM cd
    GROUP BY 1, 2, 3
)

,exposed_app_stats_cte AS (
    SELECT
        v.country
        ,l.auds_cate
        ,CASE
            WHEN v.imps > 19 THEN 20
            ELSE v.imps
        END AS exposed_impression_group
        ,COUNT(DISTINCT v.vtifa) AS exposed_app_user_counts
    FROM liveramp_matched_app_usg l
        JOIN exposed_vtifa_cte v USING (country, vtifa)
    GROUP BY 1, 2, 3
)

,impressions_cte AS (
    SELECT
        e.country
        ,e.vao
        ,CASE
            WHEN e.imps > 19 
            THEN 20
            ELSE e.imps
        END AS exposed_impression_group
        ,COUNT(DISTINCT e.vtifa) AS reach
    FROM exposed_vtifa_cte e
    GROUP BY 1, 2, 3
)

,app_stats_cte AS (
    SELECT
        l.country
        ,COUNT(DISTINCT l.vtifa) AS total_app_user_counts
    FROM liveramp_matched_app_usg l
    GROUP BY 1
)

SELECT DISTINCT
    i.vao
    ,e.auds_cate
    ,i.exposed_impression_group
    ,e.exposed_app_user_counts
    ,i.reach
    ,a.total_app_user_counts
    ,u.total_universe
    ,u.total_universe - i.reach AS total_unexpd_uni_superset
    ,a.total_app_user_counts - e.exposed_app_user_counts AS unexposed_app_user_counts
    ,CAST(e.exposed_app_user_counts AS FLOAT) / i.reach AS expd_conv_rate
    ,CAST(unexposed_app_user_counts AS FLOAT) / total_unexpd_uni_superset AS unexpd_conv_rate
    ,expd_conv_rate / unexpd_conv_rate - 1  AS lift
FROM exposed_app_stats_cte AS e
    JOIN impressions_cte i USING (country, exposed_impression_group)
    JOIN app_stats_cte a USING (country)
    JOIN universe_size u USING (country)
ORDER BY 
    i.exposed_impression_group
;


/**
Lift by Line item / Placement
*************************************************************************************************************************************************************/
WITH exposed_vtifa_cte AS (
    SELECT DISTINCT
        country
        ,vao
        ,vtifa
        ,line_item_id
        ,line_item_name
    FROM cd
)
        
,exposed_app_stats_cte AS (
    SELECT
        v.country
        ,l.auds_cate
        ,v.line_item_id AS campaign_id                                          --> refactor, BAD alias
        ,v.line_item_name AS campaign_name                                      --> refactor, BAD alias
        ,COUNT(DISTINCT v.vtifa) AS exposed_app_user_counts
        ,SUM(l.hour_spent_conv) AS exposed_time_spent_hours
    FROM liveramp_matched_app_usg l
        JOIN exposed_vtifa_cte v USING (country, vtifa)
    GROUP BY 1, 2, 3, 4
)

,impressions_cte AS (
    SELECT
        country
        ,vao
        ,line_item_id AS campaign_id
        ,line_item_name AS campaign_name
        ,COUNT(DISTINCT vtifa) AS reach
        ,SUM(imps) AS imps
    FROM cd
    GROUP BY 1, 2, 3, 4
)

,app_stats_cte AS (
    SELECT
        l.country
        ,COUNT(DISTINCT l.vtifa) AS total_app_user_counts
        ,SUM(l.hour_spent_conv) AS total_time_spent_hours
    FROM liveramp_matched_app_usg l
    GROUP BY 1
)

SELECT DISTINCT
    i.vao
    ,e.auds_cate
    ,i.campaign_name
    ,e.exposed_app_user_counts
    ,e.exposed_time_spent_hours
    ,i.reach
    ,i.imps
    ,CAST(i.imps AS FLOAT)/i.reach AS freq
    ,a.total_app_user_counts
    ,a.total_time_spent_hours
    ,u.total_universe
    ,u.total_universe - i.reach AS total_unexpd_uni_superset
    ,a.total_app_user_counts - e.exposed_app_user_counts AS unexposed_app_user_counts
    ,a.total_time_spent_hours - e.exposed_time_spent_hours AS unexposed_time_spent_hours
    ,CAST(e.exposed_app_user_counts AS FLOAT) / i.reach AS expd_conv_rate
    ,CAST(unexposed_app_user_counts AS FLOAT) / total_unexpd_uni_superset AS unexpd_conv_rate
    ,CAST(unexposed_time_spent_hours AS FLOAT) / unexposed_app_user_counts AS avg_unexpd_hour_spent
    ,CAST(e.exposed_time_spent_hours AS FLOAT) / e.exposed_app_user_counts AS avg_expd_hour_spent
    ,expd_conv_rate / unexpd_conv_rate - 1  AS lift
    ,avg_expd_hour_spent / avg_unexpd_hour_spent - 1 AS lift_time_spent
FROM exposed_app_stats_cte e
    JOIN impressions_cte i USING (country, campaign_id, campaign_name)
    JOIN app_stats_cte a USING (country)
    JOIN universe_size u USING (country)
ORDER BY 
    i.campaign_name
;


/**
Lift by Creative
*************************************************************************************************************************************************************/
WITH exposed_vtifa_cte AS (
    SELECT DISTINCT
        country
        ,vao
        ,vtifa
        ,creative_id
        ,creative_name
    FROM cd
)

,exposed_app_stats_cte AS (
    SELECT
        v.country
        ,l.auds_cate
        ,v.creative_id AS campaign_id                                           --> refactor, BAD alias
        ,v.creative_name AS campaign_name                                       --> refactor, BAD alias
        ,COUNT(DISTINCT v.vtifa) AS exposed_app_user_counts
        ,SUM(l.hour_spent_conv) AS exposed_time_spent_hours
    FROM liveramp_matched_app_usg l
        JOIN exposed_vtifa_cte v USING (country, vtifa)
    GROUP BY 1, 2, 3, 4
)

,impressions_cte AS (
    SELECT
        country
        ,vao
        ,creative_id AS campaign_id
        ,creative_name AS campaign_name
        ,COUNT(DISTINCT vtifa) AS reach
        ,SUM(imps) AS imps
    FROM cd
    GROUP BY 1, 2, 3, 4
)

,app_stats_cte AS (
    SELECT
        l.country
        ,COUNT(DISTINCT l.vtifa) AS total_app_user_counts
        ,SUM(l.hour_spent_conv) AS total_time_spent_hours
    FROM liveramp_matched_app_usg l
    GROUP BY 1
)

SELECT DISTINCT
    vao
    ,e.auds_cate
    ,i.campaign_name
    ,e.exposed_app_user_counts
    ,e.exposed_time_spent_hours
    ,i.reach
    ,i.imps
    ,CAST(i.imps AS FLOAT)/i.reach AS freq
    ,a.total_app_user_counts
    ,a.total_time_spent_hours
    ,u.total_universe
    ,u.total_universe - i.reach AS total_unexpd_uni_superset
    ,a.total_app_user_counts - e.exposed_app_user_counts AS unexposed_app_user_counts
    ,a.total_time_spent_hours - e.exposed_time_spent_hours AS unexposed_time_spent_hours
    ,CAST(e.exposed_app_user_counts AS FLOAT) / i.reach AS expd_conv_rate
    ,CAST(unexposed_app_user_counts AS FLOAT) / total_unexpd_uni_superset AS unexpd_conv_rate
    ,CAST(unexposed_time_spent_hours AS FLOAT) / unexposed_app_user_counts AS avg_unexpd_hour_spent
    ,CAST(e.exposed_time_spent_hours AS FLOAT) / e.exposed_app_user_counts AS avg_expd_hour_spent
    ,expd_conv_rate / unexpd_conv_rate - 1  AS lift
    ,avg_expd_hour_spent / avg_unexpd_hour_spent - 1 AS lift_time_spent
FROM exposed_app_stats_cte e
    JOIN impressions_cte i USING (country, campaign_id, campaign_name)
    JOIN app_stats_cte a USING (country)
    JOIN universe_size u USING (country)
ORDER BY 
    i.campaign_name
;


/**
Lift by Flight
*************************************************************************************************************************************************************/
WITH exposed_vtifa_cte AS (
    SELECT DISTINCT
        country
        ,vao
        ,vtifa
        ,flight_id
        ,flight_name
    FROM cd
)

,exposed_app_stats_cte AS (
    SELECT
        v.country
        ,l.auds_cate
        ,v.flight_id
        ,v.flight_name
        ,COUNT(DISTINCT v.vtifa) AS exposed_app_user_counts
        ,SUM(l.hour_spent_conv) AS exposed_time_spent_hours
    FROM liveramp_matched_app_usg l
        JOIN exposed_vtifa_cte v USING (country, vtifa)
    GROUP BY 1, 2, 3, 4
)

,impressions_cte AS (
    SELECT
        country
        ,vao
        ,flight_id
        ,flight_name
        ,COUNT(DISTINCT vtifa) AS reach
        ,SUM(imps) AS imps
    FROM cd
    GROUP BY 1, 2, 3, 4
)

,app_stats_cte AS (
    SELECT
        l.country
        ,COUNT(DISTINCT l.vtifa) AS total_app_user_counts
        ,SUM(l.hour_spent_conv) AS total_time_spent_hours
    FROM liveramp_matched_app_usg l
    GROUP BY 1
)

SELECT DISTINCT
    i.vao
    ,e.auds_cate
    ,i.flight_name
    ,e.exposed_app_user_counts
    ,e.exposed_time_spent_hours
    ,i.reach
    ,i.imps
    ,CAST(i.imps AS FLOAT)/i.reach AS freq
    ,a.total_app_user_counts
    ,a.total_time_spent_hours
    ,u.total_universe
    ,u.total_universe - i.reach AS total_unexpd_uni_superset
    ,a.total_app_user_counts - e.exposed_app_user_counts AS unexposed_app_user_counts
    ,a.total_time_spent_hours - e.exposed_time_spent_hours AS unexposed_time_spent_hours
    ,CAST(e.exposed_app_user_counts AS FLOAT) / i.reach AS expd_conv_rate
    ,CAST(unexposed_app_user_counts AS FLOAT) / total_unexpd_uni_superset AS unexpd_conv_rate
    ,CAST(unexposed_time_spent_hours AS FLOAT) / unexposed_app_user_counts AS avg_unexpd_hour_spent
    ,CAST(e.exposed_time_spent_hours AS FLOAT) / e.exposed_app_user_counts AS avg_expd_hour_spent
    ,expd_conv_rate / unexpd_conv_rate - 1  AS lift
    ,avg_expd_hour_spent / avg_unexpd_hour_spent - 1 AS lift_time_spent
FROM exposed_app_stats_cte e
    JOIN impressions_cte i USING (country, flight_id, flight_name)
    JOIN app_stats_cte a USING (country)
    JOIN universe_size u USING (country)
ORDER BY 
    i.flight_name
;


/**
Daily Exposure
*************************************************************************************************************************************************************/
WITH app_stats_cte AS (
    SELECT
        l.country
        ,l.auds_cate
        ,DATE_TRUNC('day', l.timing) AS timing_date
        ,COUNT(DISTINCT l.vtifa) AS total_app_user_counts
    FROM liveramp_matched_app_usg l
    GROUP BY 1, 2, 3
)

,impressions_cte AS (
    SELECT
        country
        ,vao
        ,DATE_TRUNC('day', timing) AS timing_date
        ,COUNT(DISTINCT vtifa) AS reach
        ,SUM(imps) AS imps
    FROM cd
    GROUP BY 1, 2, 3
)

,exposed_app_stats_cte AS (
    SELECT
        l.country
        ,DATE_TRUNC('day', l.timing) AS timing_date
        ,COUNT(DISTINCT l.vtifa) AS exposed_app_user_counts
    FROM liveramp_matched_app_usg l
    JOIN cd ON l.country = cd.country
        AND l.vtifa = cd.vtifa
        AND DATE_TRUNC('day', l.timing) = DATE_TRUNC('day', cd.timing)
    GROUP BY 1, 2
)

SELECT DISTINCT
    i.vao
    ,a.timing_date
    ,a.auds_cate
    ,a.total_app_user_counts
    ,e.exposed_app_user_counts
    ,i.reach
    ,i.imps
    ,u.total_universe
    ,u.total_universe - i.reach AS total_unexpd_uni_superset
    ,a.total_app_user_counts - e.exposed_app_user_counts AS unexposed_app_user_counts
FROM app_stats_cte a
    JOIN impressions_cte i USING (country, timing_date)
    JOIN exposed_app_stats_cte e USING (country, timing_date)
    JOIN universe_size u USING (country)
ORDER BY 
    a.timing_date
;


