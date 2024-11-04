/**
https://www.utctime.net/utc-to-cst-converter
https://crontab.guru/#0_2_*_*_1

------------------------------
See max time limit; Example:
------------------------------
-- Default: 3600000 (User task execution timeout in milliseconds) = 60 minutes
SHOW PARAMETERS LIKE '%USER_TASK_TIMEOUT_MS%' IN TASK <task name>;

Note: when tasks time out, it doesn't trigger slack exception notification.

To change time limit 
  1. SUSPEND the task if it is running
  2. ALTER the USER_TASK_TIMEOUT_MS. Set it to 0 for maxt time allowed or specify a number using 
     this converter:  https://my.homecampus.com.sg/Learn/Hours-to-Milliseconds-Converter
  3. RESUME the task
  4. SHOW PARAMETERS to verify the change took

All commands are listed undeer each task.

**/

--===============================
-- PARAMOUNT+ MAP 
--===============================
-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- create paramount map task
-- Every Monday at 2 AM UTC
CREATE OR REPLACE TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping
  WAREHOUSE = 'UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM'
  SCHEDULE = 'USING CRON  10 2 * * 1 UTC'
AS 
  CALL udw_clientsolutions_cs.sp_update_custom_creative_mapping(
    sales_order_source_table => 'udw_clientsolutions_cs.paramount_operative_sales_orders'
    ,cdw_exposure_source_table => 'udw_clientsolutions_cs.paramount_custom_global_exposure'
    ,destination_table => 'udw_clientsolutions_cs.paramount_custom_creative_mapping'
    ,s3_stage_path => '@udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/mapping/<date>_paramount_mapping.csv'
);

/**

-- increase default time limit to 8 hours
ALTER TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping SET USER_TASK_TIMEOUT_MS = 28800000;

-- start or restart task
ALTER TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping RESUME;

-- stop task
ALTER TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping SUSPEND;

-- check time limit
SHOW PARAMETERS LIKE '%USER_TASK_TIMEOUT_MS%' IN TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping;

-- show tasks
SHOW TASKS;

-- manually execute task
EXECUTE TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping;

-- check status
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME;
**/


-- ==============================================================================
-- ==============================================================================


--===============================
-- PLUTO MAP 
--===============================
-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- create pluto map task
-- Every Monday at 2 AM UTC
CREATE OR REPLACE TASK udw_clientsolutions_cs.tsk_update_pluto_creative_mapping
  WAREHOUSE = 'UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM'
  SCHEDULE = 'USING CRON  0 2 * * 1 UTC'
AS 
  CALL udw_clientsolutions_cs.sp_update_custom_creative_mapping(
    sales_order_source_table => 'udw_clientsolutions_cs.pluto_operative_sales_orders'
    ,cdw_exposure_source_table => 'udw_clientsolutions_cs.pluto_custom_global_exposure'
    ,destination_table => 'udw_clientsolutions_cs.pluto_custom_creative_mapping'
    ,s3_stage_path => '@udw_clientsolutions_cs.samsung_ads_data_share/analytics/custom/vaughn/mapping/<date>_pluto_mapping.csv'
);

/**

-- increase default time limit to 8 hours
ALTER TASK udw_clientsolutions_cs.tsk_update_pluto_creative_mapping SET USER_TASK_TIMEOUT_MS = 28800000;

-- start or restart task
ALTER TASK udw_clientsolutions_cs.tsk_update_pluto_creative_mapping RESUME;

-- stop task
ALTER TASK udw_clientsolutions_cs.tsk_update_pluto_creative_mapping SUSPEND;

-- check time limit
SHOW PARAMETERS LIKE '%USER_TASK_TIMEOUT_MS%' IN TASK udw_clientsolutions_cs.tsk_update_pluto_creative_mapping;

-- show tasks
SHOW TASKS;

-- manually execute task
EXECUTE TASK udw_clientsolutions_cs.tsk_update_pluto_creative_mapping;

-- check status
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME;
**/


-- ==============================================================================
-- ==============================================================================


--===============================
-- PARAMOUNT+ REPORT
--===============================
-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- send reports
-- Every Monday at 1 AM UTC / 7 AM CST
CREATE OR REPLACE TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports
  WAREHOUSE = 'UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM'
  SCHEDULE = 'USING CRON  10 13 * * 1 UTC'
AS 
  CALL udw_clientsolutions_cs.sp_paramount_get_weekly_reports();

/**

-- increase default time limit to 8 hours
ALTER TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports SET USER_TASK_TIMEOUT_MS = 28800000;

-- start or restart task
ALTER TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports RESUME;

-- stop task
ALTER TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports SUSPEND;

-- check time limit
SHOW PARAMETERS LIKE '%USER_TASK_TIMEOUT_MS%' IN TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports;

-- show tasks
SHOW TASKS;

-- manually execute task
EXECUTE TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports;

-- check status
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME;
**/


-- ==============================================================================
-- ==============================================================================


--===============================
-- PLUTO REPORT
--===============================
-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- send reports
-- Every Monday at 1 AM UTC / 7 AM CST
CREATE OR REPLACE TASK udw_clientsolutions_cs.tsk_pluto_get_weekly_reports
  WAREHOUSE = 'UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM'
  SCHEDULE = 'USING CRON  0 13 * * 1 UTC'
AS 
  CALL udw_clientsolutions_cs.sp_pluto_get_weekly_reports();

/**

-- increase default time limit to 8 hours
ALTER TASK udw_clientsolutions_cs.tsk_pluto_get_weekly_reports SET USER_TASK_TIMEOUT_MS = 28800000;

-- start or restart task
ALTER TASK udw_clientsolutions_cs.tsk_pluto_get_weekly_reports RESUME;

-- stop task
ALTER TASK udw_clientsolutions_cs.tsk_pluto_get_weekly_reports SUSPEND;

-- check time limit
SHOW PARAMETERS LIKE '%USER_TASK_TIMEOUT_MS%' IN TASK udw_clientsolutions_cs.tsk_pluto_get_weekly_reports;

-- show tasks
SHOW TASKS;

-- manually execute task
EXECUTE TASK udw_clientsolutions_cs.tsk_pluto_get_weekly_reports;

-- check status
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME;
**/



