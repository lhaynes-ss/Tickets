# Windows Task Guideline
The following are settings to select for creating tasks in Windows Task Scheduler.

## General
- Name: Task Name
- Run whether logged in or not
- Configure for: Windows Vista, windows Server 2008

## Trigger
- Begin the task: On a schedule
- Specify times
- Enabled

## Actions
- Action: Start a program
- Program/script: Path to python (E.g., C:\Users\l.haynes\AppData\Local\Microsoft\WindowsApps\python.exe)
- Add arguments (optional): Name of script (E.g., pluto_report_exposures.py) 
- Start in (optional): Path to script (E.g., C:\Users\l.haynes\Documents\Samsung\R&D\Pplus Pluto unified template\pplus_pluto_unified\local_scripts)

## Conditions
- Wake the computer to run this task
- Start only if the following network connection is available: Any connection

## Settings
- Allow task to be run on demand
- Run task as soon as possible after a scheduled start is missed
- If the task fails, restart every: 30 minutes, Attempt to restart up to: 1 times
- Stop the task if it runs longer than: 4 hours
- If the running task does not end when requested, force it to stop



# Schedule
The table below give details for the tasks that need to be scheduled. Swap $ with the content partners name (e.g., $_file.txt would be paramount_plus_file.txt or pluto_file.txt)

|Task|Task Type|File/Process|Frequency|Start Time|
|--|--|--|--|--|
|Update Mapping File|Snowflake Task|tsk_update_paramount_creative_mapping|Sunday|8:10 PM CST / Mon 2:10 AM UTC|
|Update Mapping File|Snowflake Task|tsk_update_pluto_creative_mapping|Sunday|8 PM CST / Mon 2 AM UTC|
|Import CDW Impressions|Local Task|$_report_exposures.py|Daily|11 PM CST*|
|Import App Usage|Local Task|$_report_app_usage.py|Daily|3 AM CST*|
|Generate Reports|Snowflake Task|tsk_paramount_get_weekly_reports|Monday|7:10 AM CST / Mon 1:10 PM UTC|
|Generate Reports|Snowflake Task|tsk_pluto_get_weekly_reports|Monday|7 AM CST / Mon 1 PM UTC|

\* The local Paramount tasks start times are delayed by 10 minutes

Note: when tasks time out, it doesn't trigger slack exception notification. See [example_init_tasks](../snowflake_scripts/example_init_tasks.sql) for instructions to increase default time limit.

## Validate that the reports ran 
Assuming that there were no error notifications, there should be 22 reports total based on the region and countries for the scripts.
```
-- 10/21 - 10/27

--1
LIST @udw_marketing_analytics_reports.pluto_external_us/ PATTERN = '.*2024-10-21_2024-10-27.*';
-- 1
LIST @udw_marketing_analytics_reports.paramount_plus_external_us/ PATTERN = '.*2024-10-21_2024-10-27.*';
-- 8
LIST @udw_marketing_analytics_reports.paramount_plus_external_international/ PATTERN = '.*2024-10-21_2024-10-27.*';
-- 12
LIST @udw_marketing_analytics_reports.pluto_external_international/ PATTERN = '.*2024-10-21_2024-10-27.*';
```

Example for how to [download the files](../download%20files.sh.txt)