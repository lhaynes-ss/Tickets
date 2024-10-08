/**************************************************

Description: Samsung Proofpoints: Cross-Selling - Progressive Specific.
We are trying to understand how significantly including Native media improves campaign performance against a number of factors (reach, conversion, etc.)

Client: Progressive
Parent Report: n/a
Created by: Vaughn Haynes

Methodologies:
Measurement KPIS            = media_ad_type, impressions, reach, incremental_reach*, website_conversions, exposed_conversion_rate, unexposed_conversion_rate, website_visitation_lift, avg_freq*
Measurement Period          = 2023-01-02 - 2023-12-29
Attribution Window          = 7 days
Last/Any Touch              = last touch
Time Sequential             = yes
Minimum App Usage Duration  = 1 min s
Lift Analysis               = "Web lift should be exposed visitation rate vs unexposed visitation rate, in target"
Lookback Window             = n/a

Markets: US, CA

Data Sources: UDW_PROD

Data Schemas: udw_clientsolutions_cs, data_ad_xdevice, profile_tv, udw_lib, data_tv_acr

Notes:
    Campaign Name: Progressive 2023 Upfront
    Website pixel ID: 43389

    * KPIS wist asterisk should not be exported in report.

    Product types:
        1. Native (First Screen) Only
        2. CTV Only
        3. STVP Only
        4. CTV & STVP (all media other than Native)
        5. CTV OR STVP AND Native
        6. All Media/Ad Types

    where multiple ad types types are listed, the relationship should be "AND" not "OR" 
    (i.e., reach for "all media" would mean they had to be exposed to CTV, STVP and Native) – with the exception of #5

    approximate runtime:    xx minutes
    github:                 xxxxxxxxxx
    confluence:             xxxxxxxxxx
    jira:                   https://adgear.atlassian.net/browse/SAI-6677
x

Todo: 
    - code review

**************************************************/

-- connection
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;


-- manual set variables below
SET (
    start_dt
    ,end_dt
    ,attribution_window_days
    ,country
    ,reporting_vao
    ,pixel_id
) = (
    '2023-06-01'                            --> start_dt: 'YYYY-MM-DD'; for reporting window
    ,'2023-06-30'                           --> end_dt: 'YYYY-MM-DD';  for reporting window
    ,7                                      --> attribution_window_days
    ,'US'                                   --> country
    ,87932                                  --> reporting_vao
    ,'43389'                                --> pixel_id
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
) = (
    ($start_dt  || ' 00:00:00')::TIMESTAMP  --> report_start_datetime
    ,($end_dt   || ' 23:59:59')::TIMESTAMP  --> report_end_datetime
);


SET report_end_datetime_with_attribution = DATEADD('DAY', $attribution_window_days, $report_end_datetime);


/**
campaign meta
-------------------
import campaign meta from csv. 

I pulled campaign meta via a separate query (get_products.sql) and stored in a csv files as this takes 2 hours to run
due to the fact that there are 39k records returned for the specified VAO for 2023.

    aws --profile scop s3 cp progressive_campaign_meta.csv s3://samsung.ads.data.share/analytics/custom/vaughn/progressive/progressive_campaign_meta.csv
**/
DROP TABLE IF EXISTS campaign_meta_raw;
CREATE TEMP TABLE campaign_meta_raw (
    vao                                                     VARCHAR(556)
    ,samsung_campaign_id                                    VARCHAR(556)
    ,sales_order_id                                         VARCHAR(556)
    ,sales_order_name                                       VARCHAR(556)
    ,order_start_date                                       VARCHAR(556)
    ,order_end_date                                         VARCHAR(556)
    ,campaign_id                                            VARCHAR(556)
    ,campaign_name                                          VARCHAR(556)
    ,rate_type                                              VARCHAR(556)
    ,net_unit_cost                                          VARCHAR(556)
    ,cmpgn_start_datetime_utc                               VARCHAR(556)
    ,cmpgn_end_datetime_utc                                 VARCHAR(556)
    ,flight_id                                              VARCHAR(556)
    ,flight_name                                            VARCHAR(556)
    ,flight_start_datetime_utc                              VARCHAR(556)
    ,flight_end_datetime_utc                                VARCHAR(556)
    ,creative_id                                            VARCHAR(556)
    ,creative_name                                          VARCHAR(556)
    ,sales_order_line_item_id                               VARCHAR(556)
    ,sales_order_line_item_name                             VARCHAR(556)
    ,sales_order_line_item_start_datetime_utc               VARCHAR(556)
    ,sales_order_line_item_end_datetime_utc                 VARCHAR(556)
    ,product_id                                             VARCHAR(556)
    ,product_name                                           VARCHAR(556)
);

COPY INTO campaign_meta_raw 
FROM @udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/progressive/progressive_campaign_meta.csv
file_format = (format_name = adbiz_data.mycsvformat3)
;


-- specify data types
DROP TABLE IF EXISTS campaign_meta;
CREATE TEMP TABLE campaign_meta AS (

    -- change varchar dates to date after format n/n/nnnn nn:nn as date
    SELECT 
        c.vao::INT AS vao
        ,c.samsung_campaign_id::VARCHAR AS samsung_campaign_id
        ,c.sales_order_id::INT AS sales_order_id
        ,c.sales_order_name::VARCHAR AS sales_order_name
        ,c.order_start_date::DATE AS order_start_date
        ,c.order_end_date::DATE AS order_end_date
        ,c.campaign_id::INT AS campaign_id
        ,c.campaign_name::VARCHAR AS campaign_name
        ,c.rate_type::VARCHAR AS rate_type
        ,c.net_unit_cost::FLOAT AS net_unit_cost
        ,c.cmpgn_start_datetime_utc::TIMESTAMP AS cmpgn_start_datetime_utc
        ,c.cmpgn_end_datetime_utc::TIMESTAMP AS cmpgn_end_datetime_utc
        ,c.flight_id::INT AS flight_id
        ,c.flight_name::VARCHAR AS flight_name
        ,c.flight_start_datetime_utc::TIMESTAMP AS flight_start_datetime_utc
        ,c.flight_end_datetime_utc::TIMESTAMP AS flight_end_datetime_utc
        ,c.creative_id::INT AS creative_id
        ,c.creative_name::VARCHAR AS creative_name
        ,c.sales_order_line_item_id::INT AS sales_order_line_item_id
        ,c.sales_order_line_item_name::VARCHAR AS sales_order_line_item_name
        ,c.sales_order_line_item_start_datetime_utc::TIMESTAMP AS sales_order_line_item_start_datetime_utc
        ,c.sales_order_line_item_end_datetime_utc::TIMESTAMP AS sales_order_line_item_end_datetime_utc
        ,c.product_id::INT AS product_id
        ,c.product_name::VARCHAR AS product_name
    FROM campaign_meta_raw c 
    WHERE 
        sales_order_line_item_start_datetime_utc < $report_end_datetime
        AND sales_order_line_item_end_datetime_utc > $report_start_datetime

);

SELECT * FROM campaign_meta LIMIT 1000;


/**
samsung universe
-------------------
this block of logic is from the query base and only formatting and necessary changes have been made.
change: 
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
		JOIN campaign_meta b ON a.campaign_id = b.campaign_id
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


SHOW VARIABLES;


/**
impressions (exposure)
---------------------------------------
typically named "cd".
pull impressions separately to minimize table size on joins
**/
DROP TABLE IF EXISTS impressions;
CREATE TEMP TABLE impressions AS (
    SELECT
        GET(fact.samsung_tvids_pii_virtual_id, 0) AS vtifa 
        ,fact.event_time AS exposure_datetime
        ,fact.device_country AS country
        ,fact.campaign_id
        ,fact.creative_id
        ,fact.flight_id
        ,fact.type              --> Integer that indicates type (impression, click, pixel)
    FROM data_ad_xdevice.fact_delivery_event_without_pii AS fact
    WHERE 
        fact.device_country = $country
        AND (fact.dropped != TRUE OR fact.dropped IS NULL)
        AND fact.campaign_id IN (SELECT DISTINCT campaign_id FROM campaign_meta)
        AND fact.creative_id IN (SELECT DISTINCT creative_id FROM campaign_meta)
        AND fact.type = 1 --> impressions
        AND fact.udw_partition_datetime BETWEEN $report_start_datetime AND $report_end_datetime 
);

-- SELECT COUNT(*) AS impression_count FROM impressions;


/**
pixels (conversions)
---------------------------------------
pull pixels separately to minimize table size on joins. pixel data is extremely expensive in 
terms of query performance. use no more than 30 days for testing. 1 year of data takes approx.
3.5 hours on medium for this query.
**/
DROP TABLE IF EXISTS pixels;
CREATE TEMP TABLE pixels AS (
    SELECT
        GET(fact.samsung_tvids_pii_virtual_id, 0) AS vtifa 
        ,fact.event_time AS exposure_datetime
        ,fact.device_country AS country
        ,fact.campaign_id
        ,fact.creative_id
        ,fact.flight_id
        ,fact.type              --> Integer that indicates type (impression, click, pixel)
        ,fact.segment_id        --> Integer that indicates segment used for when pixel type
    FROM data_ad_xdevice.fact_delivery_event_without_pii AS fact 
    WHERE 
        fact.device_country = $country
        AND (fact.dropped != TRUE OR fact.dropped IS NULL)
        AND fact.type = 3       --> web pixel
        AND COALESCE($pixel_id, '') <> ''
        AND fact.segment_id IN ($pixel_id)
        AND fact.udw_partition_datetime BETWEEN $report_start_datetime AND $report_end_datetime_with_attribution
  
);

-- SELECT COUNT(*) AS pixel_count FROM pixels;


/**
product data
--------------------
join impressions on campaign meta to associate product types to impressions.  

    18	    = CTV - US
    131	    = TV Plus RON (30sec) - US
    1268	= First Screen All Years - US

**/
DROP TABLE IF EXISTS impressions_with_product;
CREATE TEMP TABLE impressions_with_product AS (

    SELECT 
        i.vtifa 
        ,i.exposure_datetime
        ,i.country
        ,i.campaign_id
        ,i.creative_id
        ,i.flight_id
        ,i.type     
        ,c.product_id
        ,c.product_name
    FROM impressions i 
        JOIN campaign_meta c ON i.campaign_id = c.campaign_id
            AND i.flight_id = c.flight_id
            AND i.creative_id = c.creative_id
    WHERE
        i.exposure_datetime BETWEEN $report_start_datetime AND $report_end_datetime 

);


/**
breakout tables
------------------
isolate users exposed to each product
**/
-- 1. Native (First Screen)
DROP TABLE IF EXISTS native_first_screen;
CREATE TEMP TABLE native_first_screen AS (

    SELECT *
    FROM impressions_with_product i 
    WHERE 
        i.product_id IN (
            1268 -- First Screen All Years - US
        )

);


-- 2. CTV
DROP TABLE IF EXISTS ctv;
CREATE TEMP TABLE ctv AS (

    SELECT *
    FROM impressions_with_product i 
    WHERE 
        i.product_id IN (
            18 -- CTV - US
        )

);


-- 3. STVP
DROP TABLE IF EXISTS stvp;
CREATE TEMP TABLE stvp AS (

    SELECT *
    FROM impressions_with_product i 
    WHERE 
        i.product_id IN (
            131 -- TV Plus RON (30sec) - US
        )

);


/**
user exposure groups
-----------------------
get users who have been exposed to individial product types OR
who have been exposed to multiple product type combinations.
**/
-- 1. Native (First Screen) Only
DROP TABLE IF EXISTS native_first_screen_only;
CREATE TEMP TABLE native_first_screen_only AS (

     -- vtifas exposed to other
    -- WITH common_cte AS (
    --     SELECT DISTINCT s.vtifa FROM stvp s
    --     UNION 
    --     SELECT DISTINCT c.vtifa FROM ctv c
            
    -- )

    SELECT *
    FROM native_first_screen n 
    -- WHERE n.vtifa NOT IN (SELECT DISTINCT cc.vtifa FROM common_cte cc)

);


-- 2. CTV Only
DROP TABLE IF EXISTS ctv_only;
CREATE TEMP TABLE ctv_only AS (

    -- vtifas exposed to other
    -- WITH common_cte AS (
    --     SELECT DISTINCT s.vtifa FROM stvp s
    --     UNION 
    --     SELECT DISTINCT n.vtifa FROM native_first_screen n 
            
    -- )

    SELECT *
    FROM ctv c 
    -- WHERE c.vtifa NOT IN (SELECT DISTINCT cc.vtifa FROM common_cte cc)

);


-- 3. STVP Only
DROP TABLE IF EXISTS stvp_only;
CREATE TEMP TABLE stvp_only AS (

    -- vtifas exposed to other
    -- WITH common_cte AS (
    --     SELECT DISTINCT c.vtifa FROM ctv c
    --     UNION 
    --     SELECT DISTINCT n.vtifa FROM native_first_screen n 
            
    -- )

    SELECT *
    FROM stvp s
    -- WHERE s.vtifa NOT IN (SELECT DISTINCT cc.vtifa FROM common_cte cc)
    
);


-- 4. CTV & STVP (all media other than Native)
DROP TABLE IF EXISTS ctv_and_stvp;
CREATE TEMP TABLE ctv_and_stvp AS (

    -- vtifas exposed to ctv and stvp
    WITH common_cte AS (
        SELECT DISTINCT c.vtifa 
        FROM ctv  c
            INNER JOIN stvp s ON s.vtifa = c.vtifa
        -- WHERE 
        --     c.vtifa NOT IN (SELECT DISTINCT n.vtifa FROM native_first_screen n)
    )

    SELECT * FROM ctv c
    WHERE c.vtifa IN (SELECT cc.vtifa FROM common_cte cc)
    UNION ALL 
    SELECT * FROM stvp s
    WHERE s.vtifa IN (SELECT cc.vtifa FROM common_cte cc)

);


-- 5. CTV OR STVP AND Native
DROP TABLE IF EXISTS ctv_or_stvp_and_native;
CREATE TEMP TABLE ctv_or_stvp_and_native AS (

    -- vtifas exposed to stvp and native
    WITH common_cte AS (
        SELECT DISTINCT s.vtifa 
        FROM stvp s
            INNER JOIN native_first_screen n ON n.vtifa = s.vtifa
    )

    SELECT * FROM ctv_only c
    UNION ALL
    SELECT * FROM stvp_only s
    WHERE s.vtifa IN (SELECT cc.vtifa FROM common_cte cc)
    UNION ALL 
    SELECT * FROM native_first_screen n
    WHERE n.vtifa IN (SELECT cc.vtifa FROM common_cte cc)

);


-- 6. All Media/Ad Types
DROP TABLE IF EXISTS all_media;
CREATE TEMP TABLE all_media AS (

    -- vtifas exposed to all products
    WITH common_cte AS (
        SELECT DISTINCT s.vtifa 
        FROM stvp s
            INNER JOIN native_first_screen n ON n.vtifa = s.vtifa
            INNER JOIN ctv c ON c.vtifa = s.vtifa
    )

    SELECT * FROM ctv c
    WHERE c.vtifa IN (SELECT cc.vtifa FROM common_cte cc)
    UNION ALL
    SELECT * FROM stvp s
    WHERE s.vtifa IN (SELECT cc.vtifa FROM common_cte cc)
    UNION ALL 
    SELECT * FROM native_first_screen n
    WHERE n.vtifa IN (SELECT cc.vtifa FROM common_cte cc)

);


-- 7. CTV OR STVP
DROP TABLE IF EXISTS ctv_or_stvp;
CREATE TEMP TABLE ctv_or_stvp AS (

    SELECT * FROM ctv_only c
    UNION ALL
    SELECT * FROM stvp_only s

);


/**
start test (2024-03-01 - 2023-03-29)
--------------------------
Native (First Screen) Only
---------------------------
impression: 186026181
pixel: 8091507

native: 31518465
ctv: 94260542
stvp: 60247174

31518465 + 94260542 + 60247174 = 186026181

ctv+stvp: 99701688
ctv or stvp_native: 138052598
all: 65343182

**/
SELECT COUNT(*) AS impressions_count            FROM impressions_with_product;
SELECT COUNT(*) AS native_count                 FROM native_first_screen_only;
SELECT COUNT(*) AS ctv_count                    FROM ctv_only;
SELECT COUNT(*) AS stvp_count                   FROM stvp_only;
SELECT COUNT(*) AS ctv_stvp_count               FROM ctv_and_stvp;
SELECT COUNT(*) AS ctv_or_stvp_and_native_count FROM ctv_or_stvp_and_native;
SELECT COUNT(*) AS ctv_or_stvp_count            FROM ctv_or_stvp;
SELECT COUNT(*) AS all_count                    FROM all_media;


/**
Output tables and lables
--------------------------
The report logic is the same for each product. I will
make variables of the only parts that change so if possible
we can create a reusable code block to make this DRY in refactoring.
**/
SET product_1       = 'Native (First Screen) Only';
SET product_1_table = 'native_first_screen_only';

SET product_2       = 'CTV Only';
SET product_2_table = 'ctv_only';

SET product_3       = 'STVP Only';
SET product_3_table = 'stvp_only';

SET product_4       = 'CTV & STVP (all media other than Native)';
SET product_4_table = 'ctv_and_stvp';

SET product_5       = 'CTV OR STVP AND Native';
SET product_5_table = 'ctv_or_stvp_and_native';

SET product_6       = 'CTV OR STVP';
SET product_6_table = 'ctv_or_stvp';

SET product_7       = 'All Media/Ad Types';
SET product_7_table = 'all_media';


/**
Reports
-----------
Reminder: The code blocks below are all the same. Only the variable
names above vary from block to block.
**/
-- ==================================================================================
-- ==================================================================================
-- product_1
DROP TABLE IF EXISTS product_1_report;
CREATE TEMP TABLE product_1_report AS (

    -- impressions for product type
    -- find and replace table name in definition below
    -- exposure table: $product_1_table
    WITH impressions_CTE AS (
        SELECT 
            COUNT(*) AS impressions
        FROM TABLE($product_1_table)
    )

    -- distinct vtifas from exposures table
    ,reach_cte AS (
        SELECT 
            COUNT(DISTINCT vtifa) AS reach
        FROM TABLE($product_1_table)
    )

    -- exposed conversions (attributed and unattributed)
    -- exposed last-touch attributed (row_num = 1) and exposed un-attributed (row_num > 1 and not in attributed)
    ,conversions_cte AS (
        SELECT 
            p.*
            ,ROW_NUMBER() OVER(PARTITION BY p.vtifa, p.exposure_datetime, p.country, p.segment_id ORDER BY e.exposure_datetime) AS row_num
        FROM pixels p                                               --> conversion
            JOIN TABLE($product_1_table) e ON e.vtifa = p.vtifa     --> exposure. using alias "e" to be able to re-use the logic
                AND e.exposure_datetime <= p.exposure_datetime
                AND DATEDIFF('DAY', e.exposure_datetime, p.exposure_datetime) <= $attribution_window_days
    )

    -- list exposed converTERS from exposed converSIONS cte
    ,converters_cte AS (
        SELECT DISTINCT vtifa
        FROM conversions_cte c
        WHERE 
            c.row_num = 1
    )

    -- count of exposed converTERS from converters_cte list above
    ,conversions_count_cte AS (
        SELECT 
            COUNT(*) AS website_converters
        FROM converters_cte
    )

    -- list of unexposed users. (Samsung universe - exposed users)
    ,unexposed_cte AS (
        SELECT DISTINCT s.vtifa
        FROM samsung_ue s
        WHERE 
            s.vtifa NOT IN (
                SELECT DISTINCT c.vtifa FROM conversions_cte c
            )
    )

    -- count of unexposed users who appear in the pixel conversions table
    ,unexposed_converters_cte AS (
        SELECT COUNT(DISTINCT u.vtifa) AS unexposed_converters
        FROM unexposed_cte u
        WHERE 
            u.vtifa IN (
                SELECT DISTINCT p.vtifa FROM pixels p
            )
    )

    -- unexposed conversion rate (unexposed converters/total unexposed audience)
    ,unexposed_conversion_rate_cte AS (
        SELECT 
            CAST(unexposed_converters AS FLOAT)/(SELECT COUNT(*) FROM unexposed_cte) AS unexposed_conversion_rate
        FROM unexposed_converters_cte uc
    )

    -- final select
    SELECT
        $product_1 AS media_ad_type
        ,i.impressions
        ,r.reach
        ,NULL AS incremental_reach
        ,cc.website_converters AS website_conversions
        ,CASE WHEN r.reach = 0 THEN 0 ELSE CAST(cc.website_converters AS FLOAT)/r.reach END AS exposed_conversion_rate
        ,uc.unexposed_conversion_rate
        ,exposed_conversion_rate/uc.unexposed_conversion_rate - 1 AS website_visitation_lift
        ,NULL AS avg_freq
    FROM impressions_cte i
        JOIN reach_cte r ON 1 = 1
        JOIN conversions_count_cte cc ON 1 = 1 
        JOIN unexposed_conversion_rate_cte uc ON 1 = 1

);

-- ==================================================================================
-- ==================================================================================


-- ==================================================================================
-- ==================================================================================
-- product_2
DROP TABLE IF EXISTS product_2_report;
CREATE TEMP TABLE product_2_report AS (

    -- impressions for product type
    -- find and replace table name in definition below
    -- exposure table: $product_2_table
    WITH impressions_CTE AS (
        SELECT 
            COUNT(*) AS impressions
        FROM TABLE($product_2_table)
    )

    -- distinct vtifas from exposures table
    ,reach_cte AS (
        SELECT 
            COUNT(DISTINCT vtifa) AS reach
        FROM TABLE($product_2_table)
    )

    -- exposed conversions (attributed and unattributed)
    -- exposed last-touch attributed (row_num = 1) and exposed un-attributed (row_num > 1 and not in attributed)
    ,conversions_cte AS (
        SELECT 
            p.*
            ,ROW_NUMBER() OVER(PARTITION BY p.vtifa, p.exposure_datetime, p.country, p.segment_id ORDER BY e.exposure_datetime) AS row_num
        FROM pixels p                                               --> conversion
            JOIN TABLE($product_2_table) e ON e.vtifa = p.vtifa     --> exposure. using alias "e" to be able to re-use the logic
                AND e.exposure_datetime <= p.exposure_datetime
                AND DATEDIFF('DAY', e.exposure_datetime, p.exposure_datetime) <= $attribution_window_days
    )

    -- list exposed converTERS from exposed converSIONS cte
    ,converters_cte AS (
        SELECT DISTINCT vtifa
        FROM conversions_cte c
        WHERE 
            c.row_num = 1
    )

    -- count of exposed converTERS from converters_cte list above
    ,conversions_count_cte AS (
        SELECT 
            COUNT(*) AS website_converters
        FROM converters_cte
    )

    -- list of unexposed users. (Samsung universe - exposed users)
    ,unexposed_cte AS (
        SELECT DISTINCT s.vtifa
        FROM samsung_ue s
        WHERE 
            s.vtifa NOT IN (
                SELECT DISTINCT c.vtifa FROM conversions_cte c
            )
    )

    -- count of unexposed users who appear in the pixel conversions table
    ,unexposed_converters_cte AS (
        SELECT COUNT(DISTINCT u.vtifa) AS unexposed_converters
        FROM unexposed_cte u
        WHERE 
            u.vtifa IN (
                SELECT DISTINCT p.vtifa FROM pixels p
            )
    )

    -- unexposed conversion rate (unexposed converters/total unexposed audience)
    ,unexposed_conversion_rate_cte AS (
        SELECT 
            CAST(unexposed_converters AS FLOAT)/(SELECT COUNT(*) FROM unexposed_cte) AS unexposed_conversion_rate
        FROM unexposed_converters_cte uc
    )

    -- final select
    SELECT
        $product_2 AS media_ad_type
        ,i.impressions
        ,r.reach
        ,NULL AS incremental_reach
        ,cc.website_converters AS website_conversions
        ,CASE WHEN r.reach = 0 THEN 0 ELSE CAST(cc.website_converters AS FLOAT)/r.reach END AS exposed_conversion_rate
        ,uc.unexposed_conversion_rate
        ,exposed_conversion_rate/uc.unexposed_conversion_rate - 1 AS website_visitation_lift
        ,NULL AS avg_freq
    FROM impressions_cte i
        JOIN reach_cte r ON 1 = 1
        JOIN conversions_count_cte cc ON 1 = 1 
        JOIN unexposed_conversion_rate_cte uc ON 1 = 1

);

-- ==================================================================================
-- ==================================================================================


-- ==================================================================================
-- ==================================================================================
-- product_3
DROP TABLE IF EXISTS product_3_report;
CREATE TEMP TABLE product_3_report AS (

    -- impressions for product type
    -- find and replace table name in definition below
    -- exposure table: $product_3_table
    WITH impressions_CTE AS (
        SELECT 
            COUNT(*) AS impressions
        FROM TABLE($product_3_table)
    )

    -- distinct vtifas from exposures table
    ,reach_cte AS (
        SELECT 
            COUNT(DISTINCT vtifa) AS reach
        FROM TABLE($product_3_table)
    )

    -- exposed conversions (attributed and unattributed)
    -- exposed last-touch attributed (row_num = 1) and exposed un-attributed (row_num > 1 and not in attributed)
    ,conversions_cte AS (
        SELECT 
            p.*
            ,ROW_NUMBER() OVER(PARTITION BY p.vtifa, p.exposure_datetime, p.country, p.segment_id ORDER BY e.exposure_datetime) AS row_num
        FROM pixels p                                               --> conversion
            JOIN TABLE($product_3_table) e ON e.vtifa = p.vtifa     --> exposure. using alias "e" to be able to re-use the logic
                AND e.exposure_datetime <= p.exposure_datetime
                AND DATEDIFF('DAY', e.exposure_datetime, p.exposure_datetime) <= $attribution_window_days
    )

    -- list exposed converTERS from exposed converSIONS cte
    ,converters_cte AS (
        SELECT DISTINCT vtifa
        FROM conversions_cte c
        WHERE 
            c.row_num = 1
    )

    -- count of exposed converTERS from converters_cte list above
    ,conversions_count_cte AS (
        SELECT 
            COUNT(*) AS website_converters
        FROM converters_cte
    )

    -- list of unexposed users. (Samsung universe - exposed users)
    ,unexposed_cte AS (
        SELECT DISTINCT s.vtifa
        FROM samsung_ue s
        WHERE 
            s.vtifa NOT IN (
                SELECT DISTINCT c.vtifa FROM conversions_cte c
            )
    )

    -- count of unexposed users who appear in the pixel conversions table
    ,unexposed_converters_cte AS (
        SELECT COUNT(DISTINCT u.vtifa) AS unexposed_converters
        FROM unexposed_cte u
        WHERE 
            u.vtifa IN (
                SELECT DISTINCT p.vtifa FROM pixels p
            )
    )

    -- unexposed conversion rate (unexposed converters/total unexposed audience)
    ,unexposed_conversion_rate_cte AS (
        SELECT 
            CAST(unexposed_converters AS FLOAT)/(SELECT COUNT(*) FROM unexposed_cte) AS unexposed_conversion_rate
        FROM unexposed_converters_cte uc
    )

    -- final select
    SELECT
        $product_3 AS media_ad_type
        ,i.impressions
        ,r.reach
        ,NULL AS incremental_reach
        ,cc.website_converters AS website_conversions
        ,CASE WHEN r.reach = 0 THEN 0 ELSE CAST(cc.website_converters AS FLOAT)/r.reach END AS exposed_conversion_rate
        ,uc.unexposed_conversion_rate
        ,exposed_conversion_rate/uc.unexposed_conversion_rate - 1 AS website_visitation_lift
        ,NULL AS avg_freq
    FROM impressions_cte i
        JOIN reach_cte r ON 1 = 1
        JOIN conversions_count_cte cc ON 1 = 1 
        JOIN unexposed_conversion_rate_cte uc ON 1 = 1

);

-- ==================================================================================
-- ==================================================================================


-- ==================================================================================
-- ==================================================================================
-- product_4
DROP TABLE IF EXISTS product_4_report;
CREATE TEMP TABLE product_4_report AS (

    -- impressions for product type
    -- find and replace table name in definition below
    -- exposure table: $product_4_table
    WITH impressions_CTE AS (
        SELECT 
            COUNT(*) AS impressions
        FROM TABLE($product_4_table)
    )

    -- distinct vtifas from exposures table
    ,reach_cte AS (
        SELECT 
            COUNT(DISTINCT vtifa) AS reach
        FROM TABLE($product_4_table)
    )

    -- exposed conversions (attributed and unattributed)
    -- exposed last-touch attributed (row_num = 1) and exposed un-attributed (row_num > 1 and not in attributed)
    ,conversions_cte AS (
        SELECT 
            p.*
            ,ROW_NUMBER() OVER(PARTITION BY p.vtifa, p.exposure_datetime, p.country, p.segment_id ORDER BY e.exposure_datetime) AS row_num
        FROM pixels p                                               --> conversion
            JOIN TABLE($product_4_table) e ON e.vtifa = p.vtifa     --> exposure. using alias "e" to be able to re-use the logic
                AND e.exposure_datetime <= p.exposure_datetime
                AND DATEDIFF('DAY', e.exposure_datetime, p.exposure_datetime) <= $attribution_window_days
    )

    -- list exposed converTERS from exposed converSIONS cte
    ,converters_cte AS (
        SELECT DISTINCT vtifa
        FROM conversions_cte c
        WHERE 
            c.row_num = 1
    )

    -- count of exposed converTERS from converters_cte list above
    ,conversions_count_cte AS (
        SELECT 
            COUNT(*) AS website_converters
        FROM converters_cte
    )

    -- list of unexposed users. (Samsung universe - exposed users)
    ,unexposed_cte AS (
        SELECT DISTINCT s.vtifa
        FROM samsung_ue s
        WHERE 
            s.vtifa NOT IN (
                SELECT DISTINCT c.vtifa FROM conversions_cte c
            )
    )

    -- count of unexposed users who appear in the pixel conversions table
    ,unexposed_converters_cte AS (
        SELECT COUNT(DISTINCT u.vtifa) AS unexposed_converters
        FROM unexposed_cte u
        WHERE 
            u.vtifa IN (
                SELECT DISTINCT p.vtifa FROM pixels p
            )
    )

    -- unexposed conversion rate (unexposed converters/total unexposed audience)
    ,unexposed_conversion_rate_cte AS (
        SELECT 
            CAST(unexposed_converters AS FLOAT)/(SELECT COUNT(*) FROM unexposed_cte) AS unexposed_conversion_rate
        FROM unexposed_converters_cte uc
    )

    -- final select
    SELECT
        $product_4 AS media_ad_type
        ,i.impressions
        ,r.reach
        ,NULL AS incremental_reach
        ,cc.website_converters AS website_conversions
        ,CASE WHEN r.reach = 0 THEN 0 ELSE CAST(cc.website_converters AS FLOAT)/r.reach END AS exposed_conversion_rate
        ,uc.unexposed_conversion_rate
        ,exposed_conversion_rate/uc.unexposed_conversion_rate - 1 AS website_visitation_lift
        ,NULL AS avg_freq
    FROM impressions_cte i
        JOIN reach_cte r ON 1 = 1
        JOIN conversions_count_cte cc ON 1 = 1 
        JOIN unexposed_conversion_rate_cte uc ON 1 = 1

);

-- ==================================================================================
-- ==================================================================================


-- ==================================================================================
-- ==================================================================================
-- product_5
DROP TABLE IF EXISTS product_5_report;
CREATE TEMP TABLE product_5_report AS (

    -- impressions for product type
    -- find and replace table name in definition below
    -- exposure table: $product_5_table
    WITH impressions_CTE AS (
        SELECT 
            COUNT(*) AS impressions
        FROM TABLE($product_5_table)
    )

    -- distinct vtifas from exposures table
    ,reach_cte AS (
        SELECT 
            COUNT(DISTINCT vtifa) AS reach
        FROM TABLE($product_5_table)
    )

    -- exposed conversions (attributed and unattributed)
    -- exposed last-touch attributed (row_num = 1) and exposed un-attributed (row_num > 1 and not in attributed)
    ,conversions_cte AS (
        SELECT 
            p.*
            ,ROW_NUMBER() OVER(PARTITION BY p.vtifa, p.exposure_datetime, p.country, p.segment_id ORDER BY e.exposure_datetime) AS row_num
        FROM pixels p                                               --> conversion
            JOIN TABLE($product_5_table) e ON e.vtifa = p.vtifa     --> exposure. using alias "e" to be able to re-use the logic
                AND e.exposure_datetime <= p.exposure_datetime
                AND DATEDIFF('DAY', e.exposure_datetime, p.exposure_datetime) <= $attribution_window_days
    )

    -- list exposed converTERS from exposed converSIONS cte
    ,converters_cte AS (
        SELECT DISTINCT vtifa
        FROM conversions_cte c
        WHERE 
            c.row_num = 1
    )

    -- count of exposed converTERS from converters_cte list above
    ,conversions_count_cte AS (
        SELECT 
            COUNT(*) AS website_converters
        FROM converters_cte
    )

    -- list of unexposed users. (Samsung universe - exposed users)
    ,unexposed_cte AS (
        SELECT DISTINCT s.vtifa
        FROM samsung_ue s
        WHERE 
            s.vtifa NOT IN (
                SELECT DISTINCT c.vtifa FROM conversions_cte c
            )
    )

    -- count of unexposed users who appear in the pixel conversions table
    ,unexposed_converters_cte AS (
        SELECT COUNT(DISTINCT u.vtifa) AS unexposed_converters
        FROM unexposed_cte u
        WHERE 
            u.vtifa IN (
                SELECT DISTINCT p.vtifa FROM pixels p
            )
    )

    -- unexposed conversion rate (unexposed converters/total unexposed audience)
    ,unexposed_conversion_rate_cte AS (
        SELECT 
            CAST(unexposed_converters AS FLOAT)/(SELECT COUNT(*) FROM unexposed_cte) AS unexposed_conversion_rate
        FROM unexposed_converters_cte uc
    )

    -- final select
    SELECT
        $product_5 AS media_ad_type
        ,i.impressions
        ,r.reach
        ,NULL AS incremental_reach
        ,cc.website_converters AS website_conversions
        ,CASE WHEN r.reach = 0 THEN 0 ELSE CAST(cc.website_converters AS FLOAT)/r.reach END AS exposed_conversion_rate
        ,uc.unexposed_conversion_rate
        ,exposed_conversion_rate/uc.unexposed_conversion_rate - 1 AS website_visitation_lift
        ,NULL AS avg_freq
    FROM impressions_cte i
        JOIN reach_cte r ON 1 = 1
        JOIN conversions_count_cte cc ON 1 = 1 
        JOIN unexposed_conversion_rate_cte uc ON 1 = 1

);

-- ==================================================================================
-- ==================================================================================


-- ==================================================================================
-- ==================================================================================
-- product_6
DROP TABLE IF EXISTS product_6_report;
CREATE TEMP TABLE product_6_report AS (

    -- impressions for product type
    -- find and replace table name in definition below
    -- exposure table: $product_6_table
    WITH impressions_CTE AS (
        SELECT 
            COUNT(*) AS impressions
        FROM TABLE($product_6_table)
    )

    -- distinct vtifas from exposures table
    ,reach_cte AS (
        SELECT 
            COUNT(DISTINCT vtifa) AS reach
        FROM TABLE($product_6_table)
    )

    -- exposed conversions (attributed and unattributed)
    -- exposed last-touch attributed (row_num = 1) and exposed un-attributed (row_num > 1 and not in attributed)
    ,conversions_cte AS (
        SELECT 
            p.*
            ,ROW_NUMBER() OVER(PARTITION BY p.vtifa, p.exposure_datetime, p.country, p.segment_id ORDER BY e.exposure_datetime) AS row_num
        FROM pixels p                                               --> conversion
            JOIN TABLE($product_6_table) e ON e.vtifa = p.vtifa     --> exposure. using alias "e" to be able to re-use the logic
                AND e.exposure_datetime <= p.exposure_datetime
                AND DATEDIFF('DAY', e.exposure_datetime, p.exposure_datetime) <= $attribution_window_days
    )

    -- list exposed converTERS from exposed converSIONS cte
    ,converters_cte AS (
        SELECT DISTINCT vtifa
        FROM conversions_cte c
        WHERE 
            c.row_num = 1
    )

    -- count of exposed converTERS from converters_cte list above
    ,conversions_count_cte AS (
        SELECT 
            COUNT(*) AS website_converters
        FROM converters_cte
    )

    -- list of unexposed users. (Samsung universe - exposed users)
    ,unexposed_cte AS (
        SELECT DISTINCT s.vtifa
        FROM samsung_ue s
        WHERE 
            s.vtifa NOT IN (
                SELECT DISTINCT c.vtifa FROM conversions_cte c
            )
    )

    -- count of unexposed users who appear in the pixel conversions table
    ,unexposed_converters_cte AS (
        SELECT COUNT(DISTINCT u.vtifa) AS unexposed_converters
        FROM unexposed_cte u
        WHERE 
            u.vtifa IN (
                SELECT DISTINCT p.vtifa FROM pixels p
            )
    )

    -- unexposed conversion rate (unexposed converters/total unexposed audience)
    ,unexposed_conversion_rate_cte AS (
        SELECT 
            CAST(unexposed_converters AS FLOAT)/(SELECT COUNT(*) FROM unexposed_cte) AS unexposed_conversion_rate
        FROM unexposed_converters_cte uc
    )

    -- final select
    SELECT
        $product_6 AS media_ad_type
        ,i.impressions
        ,r.reach
        ,NULL AS incremental_reach
        ,cc.website_converters AS website_conversions
        ,CASE WHEN r.reach = 0 THEN 0 ELSE CAST(cc.website_converters AS FLOAT)/r.reach END AS exposed_conversion_rate
        ,uc.unexposed_conversion_rate
        ,exposed_conversion_rate/uc.unexposed_conversion_rate - 1 AS website_visitation_lift
        ,NULL AS avg_freq
    FROM impressions_cte i
        JOIN reach_cte r ON 1 = 1
        JOIN conversions_count_cte cc ON 1 = 1 
        JOIN unexposed_conversion_rate_cte uc ON 1 = 1

);

-- ==================================================================================
-- ==================================================================================


-- ==================================================================================
-- ==================================================================================
-- product_7
DROP TABLE IF EXISTS product_7_report;
CREATE TEMP TABLE product_7_report AS (

    -- impressions for product type
    -- find and replace table name in definition below
    -- exposure table: $product_7_table
    WITH impressions_CTE AS (
        SELECT 
            COUNT(*) AS impressions
        FROM TABLE($product_7_table)
    )

    -- distinct vtifas from exposures table
    ,reach_cte AS (
        SELECT 
            COUNT(DISTINCT vtifa) AS reach
        FROM TABLE($product_7_table)
    )

    -- exposed conversions (attributed and unattributed)
    -- exposed last-touch attributed (row_num = 1) and exposed un-attributed (row_num > 1 and not in attributed)
    ,conversions_cte AS (
        SELECT 
            p.*
            ,ROW_NUMBER() OVER(PARTITION BY p.vtifa, p.exposure_datetime, p.country, p.segment_id ORDER BY e.exposure_datetime) AS row_num
        FROM pixels p                                               --> conversion
            JOIN TABLE($product_7_table) e ON e.vtifa = p.vtifa     --> exposure. using alias "e" to be able to re-use the logic
                AND e.exposure_datetime <= p.exposure_datetime
                AND DATEDIFF('DAY', e.exposure_datetime, p.exposure_datetime) <= $attribution_window_days
    )

    -- list exposed converTERS from exposed converSIONS cte
    ,converters_cte AS (
        SELECT DISTINCT vtifa
        FROM conversions_cte c
        WHERE 
            c.row_num = 1
    )

    -- count of exposed converTERS from converters_cte list above
    ,conversions_count_cte AS (
        SELECT 
            COUNT(*) AS website_converters
        FROM converters_cte
    )

    -- list of unexposed users. (Samsung universe - exposed users)
    ,unexposed_cte AS (
        SELECT DISTINCT s.vtifa
        FROM samsung_ue s
        WHERE 
            s.vtifa NOT IN (
                SELECT DISTINCT c.vtifa FROM conversions_cte c
            )
    )

    -- count of unexposed users who appear in the pixel conversions table
    ,unexposed_converters_cte AS (
        SELECT COUNT(DISTINCT u.vtifa) AS unexposed_converters
        FROM unexposed_cte u
        WHERE 
            u.vtifa IN (
                SELECT DISTINCT p.vtifa FROM pixels p
            )
    )

    -- unexposed conversion rate (unexposed converters/total unexposed audience)
    ,unexposed_conversion_rate_cte AS (
        SELECT 
            CAST(unexposed_converters AS FLOAT)/(SELECT COUNT(*) FROM unexposed_cte) AS unexposed_conversion_rate
        FROM unexposed_converters_cte uc
    )

    -- final select
    SELECT
        $product_7 AS media_ad_type
        ,i.impressions
        ,r.reach
        ,NULL AS incremental_reach
        ,cc.website_converters AS website_conversions
        ,CASE WHEN r.reach = 0 THEN 0 ELSE CAST(cc.website_converters AS FLOAT)/r.reach END AS exposed_conversion_rate
        ,uc.unexposed_conversion_rate
        ,exposed_conversion_rate/uc.unexposed_conversion_rate - 1 AS website_visitation_lift
        ,NULL AS avg_freq
    FROM impressions_cte i
        JOIN reach_cte r ON 1 = 1
        JOIN conversions_count_cte cc ON 1 = 1 
        JOIN unexposed_conversion_rate_cte uc ON 1 = 1

);

-- ==================================================================================
-- ==================================================================================


/**
final selection
------------------
merge lift metrics for all product type combinations
**/
SELECT * FROM product_1_report      --> native_first_screen_report
UNION ALL 
SELECT * FROM product_2_report      --> ctv_only_report
UNION ALL 
SELECT * FROM product_3_report      --> stvp_only_report
UNION ALL 
SELECT * FROM product_4_report      --> ctv_and_stvp_report
UNION ALL 
SELECT * FROM product_5_report      --> ctv_or_stvp_and_native_report
UNION ALL 
SELECT * FROM product_6_report      --> ctv_or_stvp
UNION ALL 
SELECT * FROM product_7_report      --> all_media_report
;


-- end of script. Prevents DBvis error if script ends with a comment
SELECT 'DONE';



