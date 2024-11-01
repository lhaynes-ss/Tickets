
-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- paramount map
CALL udw_clientsolutions_cs.sp_update_custom_creative_mapping(
    sales_order_source_table => 'udw_clientsolutions_cs.paramount_operative_sales_orders'
    ,cdw_exposure_source_table => 'udw_clientsolutions_cs.paramount_custom_global_exposure'
    ,destination_table => 'udw_clientsolutions_cs.paramount_custom_creative_mapping'
    ,s3_stage_path => '@udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/mapping/<date>_paramount_mapping.csv'
);

SELECT * FROM udw_clientsolutions_cs.paramount_custom_creative_mapping;



--======================================================================
-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- pluto map
CALL udw_clientsolutions_cs.sp_update_custom_creative_mapping(
    sales_order_source_table => 'udw_clientsolutions_cs.pluto_operative_sales_orders'
    ,cdw_exposure_source_table => 'udw_clientsolutions_cs.pluto_custom_global_exposure'
    ,destination_table => 'udw_clientsolutions_cs.pluto_custom_creative_mapping'
    ,s3_stage_path => '@udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/mapping/<date>_pluto_mapping.csv'
);

SELECT * FROM udw_clientsolutions_cs.pluto_custom_creative_mapping;