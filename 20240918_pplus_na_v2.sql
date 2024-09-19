/**************************************************

Description: Paramount+ US and Canada Weekly reports. Report will be run every Monday and will measure the past two weeks. 
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

Data Schemas: udw_clientsolutions_cs, xxxxxxxx

Notes:
    Measure two weeks ago (M - Sun) + 7 day attribution window.

    approximate runtime:    35 minutes
    github:                 https://github.com/lhaynes-ss/Tickets/blob/main/20240918_pplus_na_v2.sql
    confluence:             https://adgear.atlassian.net/wiki/spaces/~71202089b033c00f994ec898e0d54bcb43fdf5/pages/20077379602/Paramount+and+Pluto+Instructions
    jira:                   https://adgear.atlassian.net/browse/SAI-5917 - Epic
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
    ,page_visit_lookback_days
    ,country
    ,campaign_name
    ,mapping_table
) = (
    'Paramount+'                                                            --> app_name
    ,'2024-09-09'                                                           --> start_dt: 'YYYY-MM-DD'; for reporting window
    ,'2024-09-15'                                                           --> end_dt: 'YYYY-MM-DD';  for reporting window
    ,7                                                                      --> attribution_window_days
    ,-18                                                                    --> lookback_months
    ,-30                                                                    --> page_visit_lookback_days
    ,'US'                                                                   --> country: [US | CA]
    ,'Paramount+_Q324 Initiatives'                                          --> campaign_name
    ,'udw_prod.udw_clientsolutions_cs.paramount_custom_creative_mapping'    --> mapping_table; UDW table that contains mapping data
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
vip map
----------
vip map to join on website metrics
get vip on most recent event time
**/
DROP TABLE IF EXISTS ip_psid_map;
CREATE TEMP TABLE ip_psid_map AS (

    WITH map_cte AS (
        SELECT
            m.vpsid
            ,f.device_ip_pii_virtual_id AS vip
            ,m.vtifa
            ,ROW_NUMBER() OVER (PARTITION BY f.device_ip_pii_virtual_id ORDER BY f.event_time DESC) AS rn
        FROM data_ad_xdevice.fact_delivery_event_without_pii f
            JOIN udw_lib.virtual_psid_tifa_mapping_v m ON GET(f.samsung_tvids_pii_virtual_id, 0) = m.vtifa
        WHERE 
            f.udw_partition_datetime BETWEEN $report_start_datetime AND $report_end_datetime_with_attribution
            AND f.type = 1 --> impression
            AND (f.dropped != TRUE OR f.dropped IS NULL)
            AND f.device_country = $country
    ) 

    SELECT DISTINCT
        m.vip
        ,m.vpsid
        ,m.vtifa
    FROM map_cte m
    WHERE m.rn = 1

);


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
        ,c.creative_name
        ,c.placement_id
        ,c.placement_name
        ,c.line_item_start_ts
        ,c.line_item_end_ts
        ,c.rate_type
        ,c.rate
        ,c.placement_impressions_booked
        ,c.booked_budget 
        ,CASE 
            WHEN f.type = 1 THEN 'impression' 
            WHEN f.type = 2 THEN 'click' 
            ELSE NULL
        END AS type_label
    FROM data_ad_xdevice.fact_delivery_event_without_pii AS f
		JOIN samsung_ue s ON s.vtifa = f.samsung_tvid_pii_virtual_id
        JOIN creative_map c ON c.campaign_id = f.campaign_id
            AND c.creative_id = f.creative_id
    WHERE 
        f.udw_partition_datetime BETWEEN $report_start_datetime AND $report_end_datetime 
        AND f.type IN (
            1   --> impressions
            ,2  --> clicks
        )
        AND (f.dropped != TRUE OR f.dropped IS NULL)
        AND f.device_country = $country

);

SELECT COUNT(*) AS exposure_count FROM exposures;


-- get click and impression stats
DROP TABLE IF EXISTS exposure_stats;
CREATE TEMP TABLE exposure_stats AS (

    SELECT
        e.exposure_datetime::DATE AS exposure_date
        ,e.creative_id
        ,e.campaign_id
        ,e.creative_name
        ,e.placement_id
        ,e.placement_name
        ,e.line_item_start_ts
        ,e.line_item_end_ts
        ,e.rate_type
        ,e.rate
        ,e.placement_impressions_booked
        ,e.booked_budget 
        ,SUM(CASE WHEN e.type_label = 'impression'  THEN 1 ELSE 0 END) AS impressions
        ,SUM(CASE WHEN e.type_label = 'click'       THEN 1 ELSE 0 END) AS clicks
    FROM exposures e
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

);

SELECT * FROM exposure_stats;


/**
page visits
--------------
get web pixel data in lookback + report window + attribution window
**/
DROP TABLE IF EXISTS page_visits;
CREATE TEMP TABLE page_visits AS (

    SELECT 
        f.event_time AS page_visit_datetime
        ,f.segment_id
        ,m.vtifa
    FROM data_ad_xdevice.fact_delivery_event_without_pii f  
        JOIN ip_psid_map m ON f.device_ip_pii_virtual_id = m.vip
        JOIN samsung_ue s ON s.vtifa = m.vtifa
    WHERE 
        f.type = 3              --> website data
        AND f.segment_id IN (
            52832               --> web pixel: https://trader.adgear.com/p/3/segments  | segment ID 52832
            ,52833              --> web pixel: https://trader.adgear.com/p/3/segments  | segment ID 52833
        ) 
        AND f.udw_partition_datetime BETWEEN DATEADD('DAYS', $page_visit_lookback_days, $report_start_datetime) AND $report_end_datetime_with_attribution
        AND f.device_country = $country

);


/**
first page visits
----------------------
get first page visit from page visits in report window + attribution window
**/
DROP TABLE IF EXISTS first_page_visits;
CREATE TEMP TABLE first_page_visits AS (

    SELECT
        p.vtifa
        ,p.segment_id
        ,MIN(p.page_visit_datetime) AS page_visit_first_open_datetime
    FROM page_visits p
    GROUP BY 1, 2
    HAVING 
        page_visit_first_open_datetime BETWEEN $report_start_datetime AND $report_end_datetime_with_attribution

);


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
        ,SUM(DATEDIFF('minutes', f.start_timestamp, f.end_timestamp)) AS time_spent_min
    FROM data_tv_acr.fact_app_usage_session_without_pii f
        LEFT JOIN udw_lib.virtual_psid_tifa_mapping_v m ON m.vpsid = f.psid_pii_virtual_id
    WHERE 
        f.app_id IN (
            SELECT DISTINCT app_id 
            FROM meta_apps.meta_taps_sra_app_lang_l 
            WHERE prod_nm = $app_name
        )
        AND f.country = $country
        AND DATEDIFF('second', f.start_timestamp, f.end_timestamp) >= 60
        AND f.udw_partition_datetime BETWEEN $lookback_datetime AND $report_end_datetime_with_attribution
    GROUP BY 1, 2, 3

);

SELECT COUNT(*) AS app_usage_count FROM app_usage;


/**
first app open
----------------------
get first app open for vtifa in report window + attribution window
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
        app_first_open_time BETWEEN $report_start_datetime AND $report_end_datetime_with_attribution
);

SELECT COUNT(*) AS first_open_counts FROM first_app_open;


-- get exposed page visits; last touch
DROP TABLE IF EXISTS exposed_page_visits;
CREATE TEMP TABLE exposed_page_visits AS (

    WITH exposed_visits_cte AS (
        SELECT
            p.page_visit_datetime::DATE AS page_visit_date
            ,p.segment_id
            ,e.creative_id
            ,e.creative_name
            ,e.placement_id
            ,e.placement_name
            ,ROW_NUMBER() OVER(PARTITION BY p.vtifa, p.segment_id, p.page_visit_datetime ORDER BY e.exposure_datetime DESC) AS row_num
        FROM exposures e
            JOIN page_visits p ON p.vtifa = e.vtifa 
                AND e.exposure_datetime <= p.page_visit_datetime
                AND DATEDIFF('DAY', e.exposure_datetime, p.page_visit_datetime) <= $attribution_window_days
                AND e.exposure_datetime BETWEEN $report_start_datetime AND $report_end_datetime
    )

    SELECT 
        e.page_visit_date
        ,e.segment_id
        ,e.creative_id
        ,e.creative_name
        ,e.placement_id
        ,e.placement_name
        ,COUNT(*) AS exposed_page_visits_count
    FROM exposed_visits_cte e
    WHERE 
        row_num = 1
    GROUP BY 
        1, 2, 3, 4, 5, 6

);


-- get exposed first page visits; last touch
DROP TABLE IF EXISTS exposed_first_page_visits;
CREATE TEMP TABLE exposed_first_page_visits AS (

    WITH exposed_first_time_visits_cte AS (
        SELECT
            p.page_visit_first_open_datetime::DATE AS page_visit_first_open_date
            ,p.segment_id
            ,e.creative_id
            ,e.creative_name
            ,e.placement_id
            ,e.placement_name
            ,ROW_NUMBER() OVER(PARTITION BY p.vtifa, p.segment_id, p.page_visit_first_open_datetime ORDER BY e.exposure_datetime DESC) AS row_num
        FROM exposures e
            JOIN first_page_visits p ON p.vtifa = e.vtifa 
                AND e.exposure_datetime <= p.page_visit_first_open_datetime
                AND DATEDIFF('DAY', e.exposure_datetime, p.page_visit_first_open_datetime) <= $attribution_window_days
                AND e.exposure_datetime BETWEEN $report_start_datetime AND $report_end_datetime
    )

    SELECT 
        e.page_visit_first_open_date
        ,e.segment_id
        ,e.creative_id
        ,e.creative_name
        ,e.placement_id
        ,e.placement_name
        ,COUNT(*) AS exposed_first_page_visits_count
    FROM exposed_first_time_visits_cte e
    WHERE 
        row_num = 1
    GROUP BY 
        1, 2, 3, 4, 5, 6

);


/**
daily page visits (exposed and unexposed)
------------------------------------------
get count of page visits by day for reporting period + attribution window
**/
DROP TABLE IF EXISTS daily_visits_table;
CREATE TEMP TABLE daily_visits_table AS (

    SELECT
        f.page_visit_first_open_datetime::DATE AS partition_date
        ,f.segment_id
        ,COUNT(*) AS daily_visits
    FROM first_page_visits f 
    WHERE 
        f.page_visit_first_open_datetime BETWEEN $report_start_datetime AND $report_end_datetime_with_attribution
    GROUP BY 1, 2

);

SELECT * FROM daily_visits_table;


/**
daily downloads (exposed and unexposed)
------------------------------------------
get count of downloads (first app use) by day for reporting period + attribution window
**/
DROP TABLE IF EXISTS daily_downloads_table;
CREATE TEMP TABLE daily_downloads_table AS (
    SELECT
        a.app_first_open_time::DATE AS app_usage_date
        ,COUNT(*) AS app_downloads
    FROM first_app_open a 
    WHERE 
        a.app_first_open_time BETWEEN $report_start_datetime AND $report_end_datetime_with_attribution
    GROUP BY 1
);

SELECT * FROM daily_downloads_table;


/**
exposed app usage
-------------------
aggregate exposed app opens counts and time spent by creative and campaign
this will be ANY TOUCH, time of conversion.
perhaps the term "converted exposures" is more appropriate than "exposed app opens" for any touch???
**/
DROP TABLE IF EXISTS exposed_app_open;
CREATE TEMP TABLE exposed_app_open AS (

    -- limit app usage data to after the first campaign start date.
    -- this excludes lookback window data
    WITH campaign_app_usage_cte AS (
        SELECT 
            a.vtifa
            ,a.vpsid
            ,a.app_usage_datetime
            ,a.time_spent_min
        FROM app_usage a
            JOIN samsung_ue s ON s.vtifa = a.vtifa
        WHERE 
            a.app_usage_datetime >= (
                SELECT MIN(c.line_item_start_ts) 
                FROM creative_map c
            )
    )

    SELECT
        u.app_usage_datetime::DATE AS app_usage_date
        ,e.creative_id
        ,e.creative_name
        ,e.placement_id
        ,e.placement_name
        ,COUNT(*) AS exposed_app_open_count
        ,SUM(u.time_spent_min) AS total_time_spent_min          --> any touch
    FROM exposures e
        JOIN campaign_app_usage_cte u ON e.vtifa = u.vtifa 
            AND e.exposure_datetime <= u.app_usage_datetime 
            AND DATEDIFF('DAY', e.exposure_datetime, u.app_usage_datetime) <= $attribution_window_days
    GROUP BY 1, 2, 3, 4, 5
);

-- SELECT * FROM exposed_app_open;


/**
exposed first opens
---------------------
aggregate exposed firt time app opens counts by creative and campaign.
this will be multi-attribution last-touch
**/
DROP TABLE IF EXISTS exposed_first_app_open;
CREATE TEMP TABLE exposed_first_app_open AS (

    -- get exposed first app opens, last touch per user (vtifa)
    WITH exposed_first_usage_cte AS (
        SELECT
            f.vtifa
            ,f.vpsid 
            ,f.app_first_open_time
            ,e.exposure_datetime
            ,e.udw_partition_datetime
            ,e.creative_id
            ,e.campaign_id
            ,e.creative_name
            ,e.placement_id
            ,e.placement_name
            ,e.line_item_start_ts
            ,e.line_item_end_ts
            ,e.type_label
            ,DATEDIFF('seconds', e.exposure_datetime, f.app_first_open_time) AS seconds_to_conversion
            ,ROW_NUMBER() OVER(PARTITION BY e.vtifa ORDER BY e.exposure_datetime DESC) AS row_num   --> last exposure for user
        FROM exposures e
            JOIN first_app_open f ON f.vtifa = e.vtifa 
                AND e.exposure_datetime <= f.app_first_open_time
                AND DATEDIFF('DAY', e.exposure_datetime, f.app_first_open_time) <= $attribution_window_days
    ) 

    -- get last touch counts
    SELECT 
        f.app_first_open_time::DATE AS date_first_open
        ,f.creative_name
        ,f.placement_name
        ,f.campaign_id
        ,f.placement_id
        ,f.creative_id
        ,COUNT(*) AS exposed_first_app_open_count
    FROM exposed_first_usage_cte f
    WHERE 
        f.row_num = 1
    GROUP BY 1, 2, 3, 4, 5, 6

);

-- SELECT * FROM exposed_first_app_open LIMIT 1000;


/**
output - download table
--------------------------
the following tables show exposed + unexposed conversions by day.
Note: date = date of conversion
**/
WITH downloads_cte AS (
    SELECT
        d.app_usage_date::DATE AS app_usage_day
        ,SUM(d.app_downloads) AS app_downloads
    FROM daily_downloads_table d
    GROUP BY 1
)

,sign_up_cte AS (
    SELECT
        v.partition_date AS visit_day
        ,SUM(v.daily_visits) AS daily_visits_signup
    FROM daily_visits_table v
    WHERE 
        v.segment_id IN (52832)  --> Website Sign Up Confirmation
    GROUP BY 1
)

,home_page_cte AS (
    SELECT
        v.partition_date AS visit_day
        ,SUM(v.daily_visits) AS daily_visits_homepage
    FROM daily_visits_table v
    WHERE 
        v.segment_id IN (52833)  --> Website homepage
    GROUP BY 1
)

SELECT DISTINCT
    app_usage_day
    ,COALESCE(d.app_downloads, 0) AS app_downloads
    ,COALESCE(s.daily_visits_signup, 0) AS daily_visits_signup
    ,COALESCE(h.daily_visits_homepage, 0) AS daily_visits_homepage
FROM downloads_cte d
    LEFT JOIN sign_up_cte s ON s.visit_day = d.app_usage_day
    LEFT JOIN home_page_cte h ON h.visit_day = d.app_usage_day
WHERE 
    d.app_usage_day BETWEEN $report_start_datetime::DATE AND $report_end_datetime_with_attribution::DATE
ORDER BY app_usage_day
;


/**
output - campaign table
--------------------------
join exposure metrics onto exposure stats.
again, visit metrics should show visits, not visitors. 1 visitor can have multiple visits.
exposed metrics are last touch except "exposed app opens" which are any touch same a old reporting
**/
WITH exposed_page_visits_cte AS (
    SELECT
        e.page_visit_date
        ,e.creative_name
        ,e.placement_name
        ,SUM(CASE WHEN e.segment_id IN (52832) THEN e.exposed_page_visits_count ELSE 0 END) AS exposed_page_visits_count_signup               --> Website Sign Up Confirmation
        ,SUM(CASE WHEN e.segment_id IN (52833) THEN e.exposed_page_visits_count ELSE 0 END) AS exposed_page_visits_count_homepage             --> Website homepage
    FROM exposed_page_visits e
    GROUP BY 1, 2, 3
)

,exposed_first_page_visits_cte AS (
    SELECT
        e.page_visit_first_open_date
        ,e.creative_name
        ,e.placement_name
        ,SUM(CASE WHEN e.segment_id IN (52832) THEN e.exposed_first_page_visits_count ELSE 0 END) AS exposed_first_page_visits_count_signup   --> Website Sign Up Confirmation
        ,SUM(CASE WHEN e.segment_id IN (52833) THEN e.exposed_first_page_visits_count ELSE 0 END) AS exposed_first_page_visits_count_homepage --> Website homepage
    FROM exposed_first_page_visits e
    GROUP BY 1, 2, 3
)

SELECT
    $campaign_name AS campaign_name
    ,e.campaign_id
    ,e.placement_id
    ,e.creative_id
    ,e.creative_name
    ,e.placement_name
    ,e.exposure_date AS date_of_delivery
    ,e.line_item_start_ts
    ,e.line_item_end_ts
    ,e.impressions
    ,e.clicks
    ,COALESCE(eo.exposed_app_open_count, 0) AS exposed_app_open_count
    ,COALESCE(ef.exposed_first_app_open_count, 0) AS exposed_first_app_open_count
    ,COALESCE(eo.total_time_spent_min/eo.exposed_app_open_count, 0) AS avg_min_spent_among_exposed
    ,COALESCE(ev.exposed_page_visits_count_signup, 0) AS exposed_page_visits_count_signup
    ,COALESCE(efv.exposed_first_page_visits_count_signup, 0) AS exposed_first_page_visits_count_signup
    ,COALESCE(ev.exposed_page_visits_count_homepage, 0) AS exposed_page_visits_count_homepage
    ,COALESCE(efv.exposed_first_page_visits_count_homepage, 0) AS exposed_first_page_visits_count_homepage
    ,CASE 
        WHEN e.rate_type = 'CPM'
        THEN e.rate
        WHEN e.rate_type = 'Flat Rate'
        THEN 
            -- for Flat Rate, there is one flat fee for a specific number of impressions
            -- CPM = booked budget/units of 1k impressions
            e.booked_budget/(CAST(e.placement_impressions_booked AS FLOAT)/1000)
        ELSE NULL
    END AS cpm
    ,CASE 
        WHEN e.rate_type = 'CPM'
        THEN 
            -- for CPM, rate is per 1000 impressions so...
            -- budget delivered = impressions delivered/1000 * rate
            (CAST(impressions AS FLOAT)/1000) * e.rate
        WHEN e.rate_type = 'Flat Rate'
        THEN 
            -- for Flat Rate, there is one flat fee for a specific number of impressions
            -- we can calculate a rate with these two values. Rate is per 1000 impressions so determine how many units of 1k impressions we have
            -- units of 1k impressions = booked impressions/1000
            -- CPM = booked budget/units of 1k impressions
            -- budget delivered = impressions delivered/1000 * CPM
            (CAST(impressions AS FLOAT)/1000) * (e.booked_budget/(CAST(e.placement_impressions_booked AS FLOAT)/1000))
        ELSE NULL
    END AS cost
    ,cost/exposed_app_open_count AS cpi
FROM exposure_stats e
    LEFT JOIN exposed_app_open eo ON eo.app_usage_date = e.exposure_date
        AND eo.creative_name = e.creative_name 
        AND eo.placement_name = e.placement_name
    LEFT JOIN exposed_first_app_open ef ON ef.date_first_open = e.exposure_date
        AND ef.creative_name = e.creative_name 
        AND ef.placement_name = e.placement_name
    LEFT JOIN exposed_page_visits_cte ev ON ev.page_visit_date = e.exposure_date
        AND ev.creative_name = e.creative_name
        AND ev.placement_name = e.placement_name
    LEFT JOIN exposed_first_page_visits_cte efv ON efv.page_visit_first_open_date = e.exposure_date
        AND efv.creative_name = e.creative_name
        AND efv.placement_name = e.placement_name
;


/**
output - time table
--------------------
shows exposed converters time usage. Converters are users who 
installed app in this reporting window + attribution
**/
-- recycle cached exposures from exposures table
DROP TABLE IF EXISTS cd;
CREATE TEMP TABLE cd AS (
    SELECT DISTINCT 
        e.vtifa
        ,e.creative_id
        ,e.campaign_id
        ,e.exposure_datetime
    FROM exposures e
    WHERE 
        e.type_label = 'impression'
);


-- get thresholds for all usage over 1 minute and 5 minutes
DROP TABLE IF EXISTS app_usage_time_thresholds;
CREATE TEMP TABLE app_usage_time_thresholds AS (
    SELECT DISTINCT
        m.vpsid
        ,m.vtifa
        ,f.start_timestamp AS app_usage_datetime
        ,CASE 
            WHEN DATEDIFF('second', f.start_timestamp, f.end_timestamp) >= 60
            THEN 1
            ELSE 0
        END AS is_greater_than_60
        ,CASE 
            WHEN DATEDIFF('second', f.start_timestamp, f.end_timestamp) >= 300
            THEN 1
            ELSE 0
        END AS is_greater_than_300
    FROM data_tv_acr.fact_app_usage_session_without_pii f
        LEFT JOIN udw_lib.virtual_psid_tifa_mapping_v m ON m.vpsid = f.psid_pii_virtual_id
    WHERE 
        f.app_id IN (
            SELECT DISTINCT app_id 
            FROM meta_apps.meta_taps_sra_app_lang_l 
            WHERE prod_nm = $app_name
        )
        AND f.country IN ($country)
        AND f.udw_partition_datetime BETWEEN $report_start_datetime AND $report_end_datetime_with_attribution
        AND DATEDIFF('second', f.start_timestamp, f.end_timestamp) >= 60
);


-- ouput time threshold metrics
-- count of exposed converters over threshold by placement
-- 1+ minute:
SELECT 
    '1min time spent' AS time_threshold
    ,cm.placement_name
    ,COUNT(DISTINCT c.vtifa) AS new_converter_count
FROM cd c
    JOIN samsung_ue s ON s.vtifa = c.vtifa
    JOIN creative_map cm ON cm.campaign_id = c.campaign_id
        AND cm.creative_id = c.creative_id
    JOIN first_app_open f ON f.vtifa = c.vtifa
    JOIN app_usage_time_thresholds a ON a.vtifa = c.vtifa 
        AND c.exposure_datetime <= a.app_usage_datetime
        AND DATEDIFF('DAY', c.exposure_datetime, a.app_usage_datetime) <= $attribution_window_days
        AND a.is_greater_than_60 = 1
WHERE 
    f.app_first_open_time BETWEEN $report_start_datetime AND $report_end_datetime_with_attribution
GROUP BY 1, 2
UNION
-- 5+ minutes:
SELECT 
    '5min time spent' AS time_threshold 
    ,cm.placement_name
    ,COUNT(DISTINCT c.vtifa) AS new_converter_count
FROM cd c
    JOIN samsung_ue s ON s.vtifa = c.vtifa
    JOIN creative_map cm ON cm.campaign_id = c.campaign_id
        AND cm.creative_id = c.creative_id
    JOIN first_app_open f ON f.vtifa = c.vtifa
    JOIN app_usage_time_thresholds a ON a.vtifa = c.vtifa 
        AND c.exposure_datetime <= a.app_usage_datetime
        AND DATEDIFF('DAY', c.exposure_datetime, a.app_usage_datetime) <= $attribution_window_days
        AND a.is_greater_than_300 = 1
WHERE 
    f.app_first_open_time BETWEEN $report_start_datetime AND $report_end_datetime_with_attribution
GROUP BY 1, 2
ORDER BY 
    time_threshold
    ,placement_name
;


