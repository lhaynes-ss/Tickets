

-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- check pluto counts
SELECT COUNT(*) 
FROM udw_clientsolutions_cs.pluto_custom_global_exposure 
LIMIT 1000
;

 
 
-- check paramount+ counts
SELECT COUNT(*)  
FROM udw_clientsolutions_cs.paramount_custom_global_exposure 
LIMIT 1000
;


-- reset paramount+
CREATE OR REPLACE TABLE udw_clientsolutions_cs.paramount_custom_global_exposure (
    vao                     INT
    ,vtifa                  VARCHAR
    ,exposure_datetime      TIMESTAMP
    ,country                VARCHAR(8)
    ,campaign_id            INT
    ,creative_id            INT
    ,flight_id              INT
    ,type                   INT
    ,date_imported          DATE
);


-- reset pluto   
CREATE OR REPLACE TABLE udw_clientsolutions_cs.pluto_custom_global_exposure (
    vao                     INT
    ,vtifa                  VARCHAR
    ,exposure_datetime      TIMESTAMP
    ,country                VARCHAR(8)
    ,campaign_id            INT
    ,creative_id            INT
    ,flight_id              INT
    ,type                   INT
    ,date_imported          DATE
);


