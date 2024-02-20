/**
 Create task.
 Cron time configuration: https://crontab.guru/

 Steps (Do on https://app.snowflake.com/ not DbVisualizer):
 1. Create stored procedure
 2. Create task for stored procedure
 3. View tasks with "SHOW TASKS"
 4. Task will be in the state "suspended" by default. Resume task with Alter task command. 
 5. View tasks with "SHOW TASKS" to confirm
**/


-- Step 2.
-- Every day at 8 AM UTC (3 AM EST)
CREATE OR REPLACE TASK udw_prod.udw_clientsolutions_cs.task_alexa_register_us
  WAREHOUSE = 'UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD'
  SCHEDULE = 'USING CRON  0 8 * * * UTC'
AS 
  CALL udw_prod.udw_clientsolutions_cs.sp_alexa_register_us();



-- Step 3
-- view tasks
SHOW TASKS;



-- Step 4
-- enable task after creation
ALTER TASK udw_prod.udw_clientsolutions_cs.task_alexa_register_us RESUME;



-- Step 5
-- view tasks
SHOW TASKS;


