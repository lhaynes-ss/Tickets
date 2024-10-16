

# Paramount+ and Pluto weekly report automation.

**Author:** Vaughn Haynes<br>
**Ticket:** https://adgear.atlassian.net/browse/SAI-6534<br>
**Confluence:** TBD

The purpose of this project is to automate the weekly reporting for Paramount+ and Pluto. North America data is stored in UDW (Snowflake) while international data is stored in CDW (Redshift). This project uses a combination of SQL and Python scripts to join data across both platforms. 


## Scripts
Note: Some scripts are similar between Paramount+ and Pluto. "$" may be used as a subsittute for the content partner's name in this documentation to streamline the documentation. 

For example, you may see "$_update.sql", instead of both "paramount_update.sql" and "pluto_update.sql".


|File|Trigger|Purpose|Description|
|--|--|--|--|
|config.ini|N/A|Generate Mapping|Stores DB credentials. Should not be committed to github|
|custom_global_exposures.py|N/A|Generate Mapping| Reusable python module to update the CDW exposures data based on input parameters|
|$_report_exposures.py|Local Task|Generate Mapping| Uses partner specific arguments to trigger update of operatvie one table and pass values to custom_global_exposures.py to pull data for regions EU, APAC, and SA|
|sp_update_custom_operative_sales_orders.sql|$_report_exposures.py|Generate Mapping|Reusable stored procedure to update partner operatvie one table based on parameters|
|sp_update_custom_creative_mapping.sql|Snowflake Task: tsk_update_$_creative_mapping|Generate Mapping|Reusable stored procedure to update mapping table based on arguments provided in task definition|
|_|_|_|_|


## Workflow

### Mapping file
1. Windows Task Scheduler runs locally and executes $_report_exposures.py files nighly. This updates the Operative One Sales Order tables as well as the CDW exposure tables.
1. Snowflake executes tasks tsk_update_$_creative_mapping on Monday mornings which updates the mapping tables. 



## Example/Test Queries

### Paramount +
```sql
-- preview mapping table
SELECT * FROM udw_prod.udw_clientsolutions_cs.paramount_custom_creative_mapping LIMIT 1000;

-- preview operative one sales order table
SELECT * FROM udw_prod.udw_clientsolutions_cs.paramount_operative_sales_orders LIMIT 1000;

-- preview CDW exposure table
SELECT COUNT(*) FROM udw_prod.udw_clientsolutions_cs.paramount_custom_global_exposure;

-- Example: select relevant mapping records for a specific time frame
SELECT * 
FROM udw_prod.udw_clientsolutions_cs.paramount_custom_creative_mapping
WHERE 
    line_item_start_ts <= '2024-08-25 23:59:59'
    AND line_item_end_ts >= '2024-08-19 00:00:00'
;

```

### Pluto
```sql
-- preview mapping table
SELECT * FROM udw_prod.udw_clientsolutions_cs.pluto_custom_creative_mapping LIMIT 1000;

-- preview operative one sales order table
SELECT * FROM udw_prod.udw_clientsolutions_cs.pluto_operative_sales_orders LIMIT 1000;

-- preview CDW exposure table
SELECT COUNT(*) FROM udw_prod.udw_clientsolutions_cs.pluto_custom_global_exposure;

-- Example: select relevant mapping records for a specific time frame
SELECT * 
FROM udw_prod.udw_clientsolutions_cs.pluto_custom_creative_mapping
WHERE 
    line_item_start_ts <= '2024-08-25 23:59:59'
    AND line_item_end_ts >= '2024-08-19 00:00:00'
;
```












