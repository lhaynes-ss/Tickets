/**************************************************

Description: Pluto US and Canada Weekly reports. Report will be run every Monday and will measure the past two weeks. 
Two Mondays ago - Sunday for exposure + the following  7 days for attribution.

Client: Pluto
Parent Report: n/a
Created by: Vaughn Haynes

Methodologies:
Measurement KPIS            = impressions, clicks, exposed app opens, time spent, downloads
Measurement Period          = Two weeks ago + attribution window 
Attribution Window          = 7 days
Last/Any Touch              = exposed app opens = any touch, first app open = last touch
Time Sequential             = yes
Minimum App Usage Duration  = 1 min s
Lift Analysis               = n/a
Lookback Window             = 12 months 

Markets: US, CA

Data Sources: UDW_PROD

Data Schemas: udw_clientsolutions_cs, data_ad_xdevice, data_tv_acr, profile_tv, udw_lib, data_tv_smarthub, adbiz_data

Notes:
    Measure two weeks ago (M - Sun) + 7 day attribution window.

    approximate runtime:    15 minutes
    github:                 https://github.com/lhaynes-ss/Tickets/blob/main/20240918_pluto_na_v2.sql
    confluence:             https://adgear.atlassian.net/wiki/spaces/~71202089b033c00f994ec898e0d54bcb43fdf5/pages/20077379602/Paramount+and+Pluto+Instructions
    jira:                   https://adgear.atlassian.net/browse/SAI-5916 - Epic
                            https://adgear.atlassian.net/browse/SAI-6721 - Code review
                            https://adgear.atlassian.net/browse/SAI-6534 - Automation project
                            https://adgear.atlassian.net/browse/SAI-6582 - Anomaly investigation

Todo: 
    - update confluence process

**************************************************/


-- connection
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;


-- manual set variables below
SET (
    app_name
    ,start_dt
    ,end_dt
    ,attribution_window_days
    ,lookback_months
    ,country
    ,mapping_table
) = (
    'Pluto TV'                                                              --> app_name
    ,'2024-09-09'                                                           --> start_dt: 'YYYY-MM-DD'; for reporting window
    ,'2024-09-15'                                                           --> end_dt: 'YYYY-MM-DD';  for reporting window
    ,7                                                                      --> attribution_window_days
    ,-12                                                                    --> lookback_months
    ,'US'                                                                   --> country: [US | CA]
    ,'udw_prod.udw_clientsolutions_cs.pluto_custom_creative_mapping'        --> mapping_table; UDW table that contains mapping data
);


/**
auto-set variables
--------------------
to limit possibility of human error, these values will be set for use
in the script from the manually entered variables above.
**/
SET (
    report_start_datetime
    ,report_end_datetime
    ,lookback_datetime
) = (
    ($start_dt  || ' 00:00:00')::TIMESTAMP                                  --> report_start_datetime
    ,($end_dt   || ' 23:59:59')::TIMESTAMP                                  --> report_end_datetime
    ,DATEADD('MONTH', $lookback_months, $start_dt)                          --> lookback_datetime (18 months before start date) 
);


SET report_end_datetime_with_attribution = DATEADD('DAY', $attribution_window_days, $report_end_datetime);


SHOW VARIABLES;


/**
import mapping data
----------------------
mapping table contains active campaigns for the quarter
**/
DROP TABLE IF EXISTS mapping_data;
CREATE TEMP TABLE mapping_data AS (

    SELECT m.* 
    FROM TABLE($mapping_table) m

);


/**
generate mapping file from mapping data
-----------------------------------------
the format of the temp table mimics the format of the 
original mapping file in available fields and with comas removed.
**/
DROP TABLE IF EXISTS creative_map;
CREATE TEMP TABLE creative_map AS (

    SELECT DISTINCT
        REPLACE(m.campaign_id, ',', ' ') AS campaign_id
        ,REPLACE(m.campaign_name, ',', ' ') AS campaign_name
        ,REPLACE(m.creative_id, ',', ' ') AS creative_id
        ,REPLACE(m.creative_name, ',', ' ') AS creative_name
        ,REPLACE(m.line_item_id, ',', ' ') AS placement_id
        ,REPLACE(m.line_item_name, ',', ' ') AS placement_name
        ,REPLACE(m.line_item_start_ts, ',', ' ') AS line_item_start_ts
        ,REPLACE(m.line_item_end_ts, ',', ' ') AS line_item_end_ts 
        ,m.rate_type
        ,m.rate
        ,m.placement_impressions_booked
        ,m.booked_budget
    FROM mapping_data m
    WHERE
        CAST(m.line_item_start_ts AS DATE) <= CAST($end_dt AS DATE) 
        AND CAST(m.line_item_end_ts AS DATE) >= CAST($start_dt AS DATE) 
        AND m.product_country_code_targeting = $country

);

SELECT * FROM creative_map;


/**
samsung universe
-------------------
this block of logic is from the query base and only formatting and necessary changes have been made.
change: 
    - campaign_meta to creative_map
    - TO_DATE(LEFT($REPORT_START_DATE,8),'YYYYMMDD') references to $report_start_datetime
    - TO_TIMESTAMP($REPORT_START_DATE,'YYYYMMDDHH') references to $report_start_datetime
    - TO_TIMESTAMP($REPORT_END_DATE,'YYYYMMDDHH') references to $report_end_datetime

**/
DROP TABLE IF EXISTS qualifier; --2 mins IN M
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
			THEN DATEADD(DAY, -30, $report_start_datetime)::TIMESTAMP 
			ELSE $report_start_datetime 
		END AS report_start_date
	FROM DATA_AD_XDEVICE.FACT_DELIVERY_EVENT_WITHOUT_PII a
		JOIN creative_map b ON a.campaign_id = b.campaign_id
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN $report_start_datetime AND $report_end_datetime
		AND TYPE = 1
		AND device_country = $country
);

SET report_start_date_qual = (SELECT report_start_date FROM qualifier);


DROP TABLE IF EXISTS samsung_ue; --5 mins IN M
CREATE TEMP TABLE samsung_ue AS (
	SELECT DISTINCT m.vtifa
	FROM PROFILE_TV.FACT_PSID_HARDWARE_WITHOUT_PII a
		JOIN UDW_LIB.VIRTUAL_PSID_TIFA_MAPPING_V m ON a.PSID_PII_VIRTUAL_ID = m.vpsid
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN $REPORT_START_DATE_QUAL AND $report_end_datetime
		AND partition_country = $COUNTRY	
	UNION
	SELECT DISTINCT 
		GET(SAMSUNG_TVIDS_PII_VIRTUAL_ID , 0) AS vtifa
	FROM DATA_AD_XDEVICE.FACT_DELIVERY_EVENT_WITHOUT_PII 	
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN $REPORT_START_DATE_QUAL AND $report_end_datetime
		AND TYPE = 1
		AND (EXCHANGE_ID = 6 OR EXCHANGE_SELLER_ID = 86)
		AND device_country = $COUNTRY
	UNION 
	SELECT DISTINCT m.vtifa
	FROM DATA_TV_SMARTHUB.FACT_APP_OPENED_EVENT_WITHOUT_PII a 
		JOIN UDW_PROD.UDW_LIB.VIRTUAL_PSID_TIFA_MAPPING_V m ON a.PSID_PII_VIRTUAL_ID = m.vpsid
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN $REPORT_START_DATE_QUAL AND $report_end_datetime
		AND partition_country = $country
);

SELECT COUNT(*) FROM samsung_ue;


/**
get exposures and counts
-------------------------
get exposure information for campaigns in the mapping file for the reporting window
**/
DROP TABLE IF EXISTS exposures;
CREATE TEMP TABLE exposures AS (

    SELECT
        f.samsung_tvid_pii_virtual_id AS vtifa
        ,f.event_time AS exposure_datetime
        ,f.udw_partition_datetime
        ,f.creative_id
        ,f.campaign_id
        ,f.flight_id
        ,CASE 
            WHEN f.type = 1 THEN 'impression' 
            WHEN f.type = 2 THEN 'click' 
            ELSE NULL
        END AS type_label
    FROM data_ad_xdevice.fact_delivery_event_without_pii AS f
		JOIN samsung_ue s ON s.vtifa = f.samsung_tvid_pii_virtual_id
    WHERE 
        f.udw_partition_datetime BETWEEN $report_start_datetime AND $report_end_datetime 
        AND f.campaign_id IN (
            SELECT DISTINCT campaign_id 
            FROM creative_map
        ) 
        AND f.type IN (
            1   --> impressions
            ,2  --> clicks
        )
        AND (f.dropped != TRUE OR f.dropped IS NULL)
        AND f.device_country = $country

);

SELECT COUNT(*) AS exposure_count FROM exposures;


-- from exposures, get impression counts
DROP TABLE IF EXISTS impression_stats;
CREATE TEMP TABLE impression_stats AS (

    SELECT  
        e.campaign_id
        ,e.flight_id
        ,e.creative_id 
        ,COALESCE(COUNT(*), 0) AS impressions
    FROM exposures e
    WHERE 
        e.type_label = 'impression'
    GROUP BY 1, 2, 3

);

-- SELECT * FROM impression_stats;


-- from exposures, get click counts
DROP TABLE IF EXISTS click_stats;
CREATE TEMP TABLE click_stats AS (

    SELECT  
        e.campaign_id
        ,e.flight_id
        ,e.creative_id 
        ,COALESCE(COUNT(*), 0) AS clicks
    FROM exposures e
    WHERE 
        e.type_label = 'click'
    GROUP BY 1, 2, 3

);

-- SELECT * FROM click_stats;


/**
get app usage
------------------
min time threshold is 60 seconds. time period includes 
lookback + report window + attribution window
**/
DROP TABLE IF EXISTS app_usage;
CREATE TEMP TABLE app_usage AS (

    SELECT
        m.vtifa
        ,m.vpsid
        ,f.start_timestamp AS app_usage_datetime
        ,f.udw_partition_datetime
        ,SUM(DATEDIFF('minutes', f.start_timestamp, f.end_timestamp)) AS time_spent_min
        ,ROUND(time_spent_min/60, 2) AS time_spent_hour
    FROM data_tv_acr.fact_app_usage_session_without_pii f
        LEFT JOIN udw_lib.virtual_psid_tifa_mapping_v m ON m.vpsid = f.psid_pii_virtual_id
    WHERE 
        f.app_id IN (
            SELECT DISTINCT app_id 
            FROM adbiz_data.lup_app_cat_genre_2023 
            WHERE prod_nm = $app_name
        ) 
        AND f.country = $country
        AND DATEDIFF('second', f.start_timestamp, f.end_timestamp) >= 60
        AND f.udw_partition_datetime BETWEEN $lookback_datetime AND $report_end_datetime_with_attribution
    GROUP BY 1, 2, 3, 4

);

SELECT COUNT(*) AS app_usage_count FROM app_usage;


/**
first app opens
-------------------
from app usage, get first app opens; filter to only those in reporting window.
**/
DROP TABLE IF EXISTS first_app_open;
CREATE TEMP TABLE first_app_open AS (

    SELECT 
        a.vtifa
        ,a.vpsid 
        ,MIN(a.app_usage_datetime) AS app_first_open_time
    FROM app_usage a
        JOIN samsung_ue s ON s.vtifa = a.vtifa
    GROUP BY 1, 2
    HAVING 
        app_first_open_time >= $report_start_datetime

);

-- SELECT COUNT(*) AS first_open_counts FROM first_app_open;


-- get count of downloads aka downloaders (first app usage) 
DROP TABLE IF EXISTS downloads_table;
CREATE TEMP TABLE downloads_table AS (

    SELECT 
        COUNT(*) AS downloads
    FROM first_app_open f

);

-- SELECT * FROM downloads_table;


/**
measurable app usage
----------------------
limit app usage data to after the first line item start date.
this excludes lookback window data
**/
DROP TABLE IF EXISTS campaign_app_usage;
CREATE TEMP TABLE campaign_app_usage AS (

    SELECT 
        a.vtifa
        ,a.vpsid
        ,a.app_usage_datetime
        ,a.udw_partition_datetime
        ,a.time_spent_min
        ,a.time_spent_hour 
    FROM app_usage a
        JOIN samsung_ue s ON s.vtifa = a.vtifa
    WHERE 
        a.app_usage_datetime >= (
            SELECT MIN(c.line_item_start_ts) 
            FROM creative_map c
        )

);


/**
reportable first app usage
----------------------------
limit first app usage data to after the first line item start date.
this excludes lookback window data
**/
DROP TABLE IF EXISTS campaign_first_app_usage;
CREATE TEMP TABLE campaign_first_app_usage AS (

    SELECT 
        f.vtifa
        ,f.vpsid 
        ,f.app_first_open_time 
    FROM first_app_open f
        JOIN samsung_ue s ON s.vtifa = f.vtifa
    WHERE 
        f.app_first_open_time >= (
            SELECT MIN(c.line_item_start_ts) 
            FROM creative_map c
        )

);


/**
exposed app opens
--------------------
aggregate exposed app opens and time spent by creative and campaign.
this will be ANY TOUCH with attribution window, time of exposure
**/
DROP TABLE IF EXISTS exposed_app_open;
CREATE TEMP TABLE exposed_app_open AS (

    WITH exposed_app_usage_cte AS (
        SELECT
            e.exposure_datetime
            ,u.time_spent_min
            ,e.creative_id
            ,e.campaign_id
            ,e.vtifa
        FROM exposures e
            JOIN campaign_app_usage u ON u.vtifa = e.vtifa
                AND e.exposure_datetime <= u.app_usage_datetime 
                AND DATEDIFF('DAY', e.exposure_datetime, u.app_usage_datetime) <= $attribution_window_days
                AND e.exposure_datetime BETWEEN $report_start_datetime AND $report_end_datetime
    )

    SELECT
        e.creative_id
        ,e.campaign_id
        ,COUNT(*) AS count_exposed_app_opens
        ,SUM(e.time_spent_min) AS total_time_spent_min
    FROM exposed_app_usage_cte e
    GROUP BY 1, 2

);

-- SELECT * FROM exposed_app_open;


/**
exposed first app opens
-------------------------
aggregate exposed fisrt time app open counts by creative and campaign.
this will be multi-attribution last-touch
**/
DROP TABLE IF EXISTS exposed_first_time_open;
CREATE TEMP TABLE exposed_first_time_open AS (

    WITH exposed_first_usage_cte AS ( 
        SELECT DISTINCT 
            fu.app_first_open_time
            ,fu.vtifa
            ,e.exposure_datetime
            ,e.creative_id
            ,e.campaign_id
            ,ROW_NUMBER() OVER(PARTITION BY fu.vtifa, fu.app_first_open_time ORDER BY e.exposure_datetime DESC) AS row_num
        FROM exposures e
            JOIN campaign_first_app_usage fu ON fu.vtifa = e.vtifa
                AND e.exposure_datetime <= fu.app_first_open_time
                AND DATEDIFF('DAY', e.exposure_datetime, fu.app_first_open_time) <= $attribution_window_days
                AND e.exposure_datetime BETWEEN $report_start_datetime AND $report_end_datetime
    )

    -- get last touch counts
    SELECT
        f.creative_id
        ,f.campaign_id
        ,COUNT(*) AS count_exposed_first_app_opens 
    FROM exposed_first_usage_cte f
    WHERE f.row_num = 1
    GROUP BY 1, 2

);

-- SELECT * FROM exposed_first_time_open LIMIT 1000;


/**
output
---------
join all stats on creative map.
stored in cte first so column order can be changed for report output
**/
WITH output_cte AS (
    SELECT 
        $app_name AS campaign_name
        ,m.campaign_id
        ,m.placement_id
        ,m.creative_id
        ,m.creative_name
        ,m.placement_name
        ,m.line_item_start_ts
        ,m.line_item_end_ts
        ,COALESCE(i.impressions, 0) AS impression
        ,COALESCE(c.clicks, 0) AS click
        ,e.count_exposed_app_opens
        ,fo.count_exposed_first_app_opens
        ,e.total_time_spent_min
        ,ROUND(e.total_time_spent_min/60, 2) total_time_spent_hour 
        ,d.downloads
        ,(CAST(click AS FLOAT)/impression) * 100 AS ctr
        ,CASE 
            WHEN m.rate_type = 'CPM'
            THEN m.rate
            WHEN m.rate_type = 'Flat Rate'
            THEN 
                -- for Flat Rate, there is one flat fee for a specific number of impressions
                -- CPM = booked budget/units of 1k impressions
                m.booked_budget/(CAST(m.placement_impressions_booked AS FLOAT)/1000)
            ELSE NULL
        END AS cpm
        ,CASE 
            WHEN m.rate_type = 'CPM'
            THEN 
                -- for CPM, rate is per 1000 impressions so...
                -- budget delivered = impressions delivered/1000 * rate
                (CAST(impression AS FLOAT)/1000) * m.rate
            WHEN m.rate_type = 'Flat Rate'
            THEN 
                -- for Flat Rate, there is one flat fee for a specific number of impressions
                -- we can calculate a rate with these two values. Rate is per 1000 impressions so determine how many units of 1k impressions we have
                -- units of 1k impressions = booked impressions/1000
                -- CPM = booked budget/units of 1k impressions
                -- budget delivered = impressions delivered/1000 * CPM
                (CAST(impression AS FLOAT)/1000) * (m.booked_budget/(CAST(m.placement_impressions_booked AS FLOAT)/1000))
            ELSE NULL
        END AS spend
        ,spend/e.count_exposed_app_opens AS cpi
        ,spend/fo.count_exposed_first_app_opens AS cac
    FROM creative_map m
        LEFT JOIN impression_stats i ON i.creative_id = m.creative_id
            AND i.campaign_id = m.campaign_id
        LEFT JOIN click_stats c ON c.creative_id = m.creative_id
            AND c.campaign_id = m.campaign_id
        LEFT JOIN exposed_app_open AS e ON e.creative_id = m.creative_id
            AND e.campaign_id = m.campaign_id
        LEFT JOIN exposed_first_time_open fo ON fo.creative_id = m.creative_id
            AND fo.campaign_id = m.campaign_id
        CROSS JOIN downloads_table d
)

SELECT 
    o.campaign_name
    ,o.campaign_id
    ,o.placement_id
    ,o.creative_id
    ,o.creative_name
    ,o.placement_name
    ,o.line_item_start_ts
    ,o.line_item_end_ts
    ,o.impression
    ,o.click
    ,o.count_exposed_app_opens
    ,o.count_exposed_first_app_opens
    ,o.total_time_spent_min
    ,o.total_time_spent_hour 
    ,o.downloads
    ,o.ctr
    ,o.cpi
    ,o.cac
    ,o.spend
FROM output_cte o
;


