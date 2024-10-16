

-- create paramount map task
-- Every Monday at 2 AM UTC
CREATE OR REPLACE TASK udw_prod.udw_clientsolutions_cs.tsk_update_paramount_creative_mapping
  WAREHOUSE = 'UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD'
  SCHEDULE = 'USING CRON  0 2 * * 1 UTC'
AS 
  CALL udw_prod.udw_clientsolutions_cs.sp_update_custom_creative_mapping(
    sales_order_source_table => 'udw_prod.udw_clientsolutions_cs.paramount_operative_sales_orders'
    ,cdw_exposure_source_table => 'udw_prod.udw_clientsolutions_cs.paramount_custom_global_exposure'
    ,destination_table => 'udw_prod.udw_clientsolutions_cs.paramount_custom_creative_mapping'
    ,s3_stage_path => '@udw_prod.udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/mapping/<date>_paramount_mapping.csv'
);

/**
-- start or restart task
ALTER TASK udw_prod.udw_clientsolutions_cs.tsk_update_paramount_creative_mapping RESUME;

-- stop task
ALTER TASK udw_prod.udw_clientsolutions_cs.tsk_update_paramount_creative_mapping SUSPEND;

-- show tasks
SHOW TASKS;

-- manually execute task
EXECUTE TASK udw_prod.udw_clientsolutions_cs.tsk_update_paramount_creative_mapping;

-- check status
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME;
**/


-- create pluto map task
-- Every Monday at 2 AM UTC
CREATE OR REPLACE TASK udw_prod.udw_clientsolutions_cs.tsk_update_pluto_creative_mapping
  WAREHOUSE = 'UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD'
  SCHEDULE = 'USING CRON  0 2 * * 1 UTC'
AS 
  CALL udw_prod.udw_clientsolutions_cs.sp_update_custom_creative_mapping(
    sales_order_source_table => 'udw_prod.udw_clientsolutions_cs.pluto_operative_sales_orders'
    ,cdw_exposure_source_table => 'udw_prod.udw_clientsolutions_cs.pluto_custom_global_exposure'
    ,destination_table => 'udw_prod.udw_clientsolutions_cs.pluto_custom_creative_mapping'
    ,s3_stage_path => '@udw_prod.udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/mapping/<date>_pluto_mapping.csv'
);

/**
-- start or restart task
ALTER TASK udw_prod.udw_clientsolutions_cs.tsk_update_pluto_creative_mapping RESUME;

-- stop task
ALTER TASK udw_prod.udw_clientsolutions_cs.tsk_update_pluto_creative_mapping SUSPEND;

-- show tasks
SHOW TASKS;

-- manually execute task
EXECUTE TASK udw_prod.udw_clientsolutions_cs.tsk_update_pluto_creative_mapping;

-- check status
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME;
**/


