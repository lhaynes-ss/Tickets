/***********************************

Generate Alexa Audience Files Daily (NASBO)

STAGES
------------
@adbiz_data.samsung_ads_data_share/analytics/custom/Kuo_Data_Lab/alexa/registers/220601_us_alexa_10per_control.csv
s3://samsung.ads.data.share/analytics/custom/Kuo_Data_Lab/alexa/registers/220601_us_alexa_10per_control.csv

@demo_stage10000/220601_us_alexa_10per_control.csv
s3://samsung.ads.data.share/analytics/custom/Kuo_Data_Lab/alexa/registers/220601_us_alexa_10per_control.csv

@udw_prod.udw_marketing_analytics_reports.audience_planner_remote_files_udw_s/230912_us_alexa_register_last_45_230912to231231.csv
s3://adgear-etl-audience-planner/remotefiles/udw/230912_us_alexa_register_last_45_230912to231231.csv


AWS:
aws --profile nyc s3 ls s3://adgear-etl-audience-planner/remotefiles/udw/230912_us_alexa_register_last_45_230912to231231.csv


CALL udw_prod.udw_clientsolutions_cs.sp_alexa_register_us_daily();

***********************************/
CREATE OR REPLACE PROCEDURE udw_prod.udw_clientsolutions_cs.sp_alexa_register_us_daily()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
DECLARE 
    yesterday_start VARCHAR;
    yesterday_end VARCHAR;
    fourtyfive_ago VARCHAR;

    message VARCHAR;
      
BEGIN     

    -- set variables
    yesterday_start := (SELECT CONCAT(TO_CHAR(DATEADD(DAY, -1, CURRENT_DATE()),'yyyymmdd'),'00'));
    yesterday_end := (SELECT CONCAT(TO_CHAR(DATEADD(DAY, -1, CURRENT_DATE()),'yyyymmdd'),'23'));
    fourtyfive_ago := (SELECT CONCAT(TO_CHAR(DATEADD(DAY, -45, CURRENT_DATE()),'yyyymmdd'),'00'));

    message := 'Process Completed';


    /***********************
     get registrations
    ***********************/
    -- registrations in last 45 days
    DROP TABLE IF EXISTS last_45_registers;
    CREATE TEMP TABLE last_45_registers AS (
        SELECT DISTINCT psid 
        FROM data_kpi_src.fact_voice_without_pii t
            JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v m1 ON t.psid_pii_virtual_id = m1.vpsid 
        WHERE 
            partition_country = 'US'
            AND category IN ('EV110')
            AND SPLIT_PART(payload, ',', 4) LIKE '%LOGIN_ACTIVATETV%'
            AND SPLIT_PART(payload, ',', 1) ILIKE '%com.samsung.tv.alexa-client%'
            AND udw_partition_datetime BETWEEN TO_TIMESTAMP(:fourtyfive_ago,'YYYYMMDDHH') AND TO_TIMESTAMP(:yesterday_end,'YYYYMMDDHH')
    );



    -- registrations today      
    DROP TABLE IF EXISTS current_day;
    CREATE TEMP TABLE current_day AS (
        SELECT DISTINCT psid 
        FROM data_kpi_src.fact_voice_without_pii t
            JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v m1 ON t.psid_pii_virtual_id = m1.vpsid 
        WHERE 
            partition_country = 'US'
            AND category IN ('EV110')
            AND SPLIT_PART(payload, ',', 4) LIKE '%LOGIN_ACTIVATETV%'
            AND SPLIT_PART(payload, ',', 1) LIKE '%com.samsung.tv.alexa-client%'
            AND udw_partition_datetime BETWEEN TO_TIMESTAMP(:yesterday_start,'YYYYMMDDHH') AND TO_TIMESTAMP(:yesterday_end,'YYYYMMDDHH')
    );



    /***********************
     create a control group
    ***********************/
    DROP TABLE IF EXISTS aa_seg_100;
    CREATE TEMP TABLE aa_seg_100 AS (
        SELECT
            psid,
            NTILE(100) OVER (ORDER BY RANDOM()) rownum
        FROM current_day
    );


    DROP TABLE IF EXISTS control;
    CREATE TEMP TABLE control AS (
        SELECT DISTINCT
            psid
        FROM aa_seg_100
        WHERE
            rownum <= 10 
    );


    -- recreated audience in 202309 but still use the old control audiences
    DROP TABLE IF EXISTS current_control;
    CREATE TEMP TABLE current_control (psid VARCHAR(556));
    COPY INTO current_control
    FROM @adbiz_data.samsung_ads_data_share/analytics/custom/Kuo_Data_Lab/alexa/registers/220601_us_alexa_10per_control.csv
    file_format = (format_name = adbiz_data.mycsvformat3);

       
    DROP TABLE IF EXISTS new_control;
    CREATE TEMP TABLE new_control AS (
        SELECT psid FROM control
        UNION
        SELECT psid FROM current_control
    );



    -- save to file
    COPY INTO @udw_marketing_analytics_reports.demo_stage10000/220601_us_alexa_10per_control.csv FROM (SELECT DISTINCT psid FROM new_control)
    file_format = (format_name = adbiz_data.mycsvformat10000 compression = 'none')
    single=true
    header = true
    max_file_size=4900000000
    OVERWRITE = TRUE;



    /***********************
     create audience
    ***********************/
    DROP TABLE IF EXISTS new_seg;
    CREATE TEMP TABLE new_seg AS (
        SELECT 
            psid
        FROM last_45_registers
        WHERE 
            psid NOT IN (
                SELECT psid
                FROM new_control
            )
    );


            
    -- save to file
    COPY INTO @udw_prod.udw_marketing_analytics_reports.audience_planner_remote_files_udw_s/230912_us_alexa_register_last_45_230912to231231.csv FROM (SELECT DISTINCT psid FROM new_seg)
    file_format = (format_name = adbiz_data.mycsvformat10000 compression = 'none')
    single = true
    header = true
    max_file_size = 4900000000
    OVERWRITE = TRUE;


    RETURN message;

END;
$$
;



