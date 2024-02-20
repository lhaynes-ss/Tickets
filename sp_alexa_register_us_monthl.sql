/***********************************

Generate Alexa Audience Files Monthly (NASBO)

Storage
------------
Old location:
s3://samsung.ads.data.share/analytics/custom/DJ_Data_Lab/Alexa/alexa_registered_20211212_20230611_1074224.csv

New Location:
s3://adgear-etl-audience-planner/remotefiles/alexa_registered_20211212_20230611_1074224.csv

AWS:
aws --profile nyc s3 ls s3://adgear-etl-audience-planner/remotefiles/alexa_registered_


CALL udw_prod.udw_clientsolutions_cs.sp_alexa_register_us_monthly();

***********************************/
CREATE OR REPLACE PROCEDURE udw_prod.udw_clientsolutions_cs.sp_alexa_register_us_monthly()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
DECLARE 
	data_retention VARCHAR;
	yesterday VARCHAR;
    reg_count VARCHAR;
    message VARCHAR;

BEGIN 


    -- set variables
	data_retention := (SELECT CONCAT(TO_CHAR(DATEADD(MONTH,-18, CURRENT_DATE()),'yyyymmdd'),'00'));
	yesterday := (SELECT CONCAT(TO_CHAR(DATEADD(DAY,-1, CURRENT_DATE()),'yyyymmdd'),'23'));
	message := 'Process Completed';

	-- SELECT $DATA_RETENTION, $YESTERDAY; --2021121200	2023061123



	/***********************
     get registrations for past 18 months
    ***********************/
	DROP TABLE IF EXISTS alexa_reg;
	CREATE TEMP TABLE alexa_reg AS (
		SELECT DISTINCT psid
		FROM data_kpi_src.fact_voice 
		WHERE partition_datehour BETWEEN :data_retention AND :yesterday
			AND partition_country = 'US'
			AND category = 'EV110'
			AND payload:appid::STRING  = 'com.samsung.tv.alexa-client'
			AND payload:exe_goal::STRING = 'LOGIN_ACTIVATETV'
	);

	reg_count := (SELECT COUNT(DISTINCT psid)::VARCHAR FROM alexa_reg); --1,074,224



	-- save to file; path is dynamically generated
    -- add space to beginning of new line
    -- Copy to s3://adgear-etl-audience-planner/remotefiles/alexa_registered_20211212_20230611_1074224.csv
	LET sql_stm := 'COPY INTO \'s3://adgear-etl-audience-planner/remotefiles/alexa_registered_' || data_retention || '_' || yesterday || '_' || reg_count || '.csv\' FROM alexa_reg'
	|| ' storage_integration = data_analytics_share'
	|| ' file_format = (format_name = adbiz_data.analytics_csv COMPRESSION = \'none\')'
	|| ' single = TRUE'
	|| ' header = TRUE'
	|| ' overwrite = TRUE'
	|| ' max_file_size = 5368709120'; 


    EXECUTE IMMEDIATE sql_stm;

    RETURN message;

END;
$$
;

