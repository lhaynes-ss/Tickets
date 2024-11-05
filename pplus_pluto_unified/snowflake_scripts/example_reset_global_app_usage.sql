/**
This shows how to reset the app usage table.
This shouldn't be needed unless you are making changes to the columns
and want to wipe the data and re=pull everything.
**/

-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- check pluto counts
SELECT COUNT(*) 
FROM udw_clientsolutions_cs.pluto_custom_app_usage 
LIMIT 1000
;


-- check paramount+ counts
SELECT COUNT(*) 
FROM udw_clientsolutions_cs.paramount_custom_app_usage 
LIMIT 1000;

/**

-- reset pluto
CREATE OR REPLACE TABLE udw_clientsolutions_cs.pluto_custom_app_usage (
    tifa                VARCHAR
    ,app_usage_datetime TIMESTAMP
    ,country            VARCHAR
    ,app_id             VARCHAR
    ,time_spent_min     BIGINT
    ,usage_count        INT
    ,date_imported      DATE
);


-- reset paramount+ 
CREATE OR REPLACE TABLE udw_clientsolutions_cs.paramount_custom_app_usage (
    tifa                VARCHAR
    ,app_usage_datetime TIMESTAMP
    ,country            VARCHAR
    ,app_id             VARCHAR
    ,time_spent_min     BIGINT
    ,usage_count        INT
    ,date_imported      DATE
);

**/

