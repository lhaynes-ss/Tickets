'''
===================================================
IMPORT EU ALEXA AUDIENCE DATA FROM CDW

Ticket: https://adgear.atlassian.net/browse/SAI-5824
Setup: https://adgear.atlassian.net/wiki/spaces/MAST/pages/19742589039/Python+Setup+on+Windows+Machine
github: https://github.com/lhaynes-ss/Tickets/blob/main/alexa_eu.py
runtime: ~1 minute

Regions
    - DE (Germany)
    - GB (United Kingdom)

s3 Locations: 
    - s3://adgear-etl-audience-planner/remotefiles/udw/230912_[de|gb]_alexa_register_last_45_230912to231231.csv
    - s3://adgear-etl-audience-planner/remotefiles/udw/220601_[de|gb]_alexa_10per_control.csv

UDW Tables: 
    - udw_prod.UDW_CLIENTSOLUTIONS_CS.cdw_[de|gb]_alexa_current_day
    - udw_prod.UDW_CLIENTSOLUTIONS_CS.cdw_[de|gb]_alexa_last_45_registers
    - udw_prod.UDW_CLIENTSOLUTIONS_CS.cdw_[de|gb]_alexa_new_control
    - udw_prod.UDW_CLIENTSOLUTIONS_CS.cdw_[de|gb]_alexa_new_segment
===================================================
'''


# import packages
import datetime
from datetime import date
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import configparser
from pathlib import Path



# get config
config = configparser.ConfigParser()
config_path = Path('C:\\Users\\l.haynes\\Documents\\Samsung\\R&D\\alexa_eu\\config.ini')
config.read(config_path)


start_timestamp = datetime.now()

#================================================================
# START OPEN DB CONNECTIONS
#================================================================

cdw_conn = psycopg2.connect(
    dbname = config["personalAccountEU"]["dbname"],
    host = config["personalAccountEU"]["host"],
    port = config["personalAccountEU"]["port"],
    user = config["personalAccountEU"]["user"],
    password = config["personalAccountEU"]["password"])
cdw_cur = cdw_conn.cursor()
print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> CDW connected")


udw_conn = snowflake.connector.connect(
    user = config["serviceAccount"]["user"],
    password = config["serviceAccount"]["password"],
    account = config["serviceAccount"]["account"],
    warehouse = config["serviceAccount"]["warehouse"],
    database = config["serviceAccount"]["database"],
    schema = config["serviceAccount"]["schema"]
)
udw_cur = udw_conn.cursor()

udw_cur.execute("USE ROLE UDW_CLIENTSOLUTIONS_REPORTING_MAINTAINER_ROLE_PROD;")
udw_cur.execute("USE DATABASE UDW_PROD;")
udw_cur.execute("USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;")
udw_cur.execute("USE SCHEMA UDW_CLIENTSOLUTIONS_CS;")

print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> UDW NA connected. UDW environment set up.")

#================================================================
# END OPEN DB CONNECTIONS
#================================================================



def yesterday_start_YYYYMMDDHH():
    r = datetime.today() - timedelta(days = 1)
    result = r.strftime('%Y-%m-%d').replace('-', '')
    return result + '00'

def yesterday_end_YYYYMMDDHH():
    r = datetime.today() - timedelta(days = 1)
    result = r.strftime('%Y-%m-%d').replace('-', '')
    return result + '23'

def fourtyfive_ago_YYYYMMDDHH():
    period = datetime.today() - timedelta(days = 45)
    period_ago = period.strftime('%Y-%m-%d').replace('-', '')
    return period_ago + '23'

yesterday_start = yesterday_start_YYYYMMDDHH()
yesterday_end = yesterday_end_YYYYMMDDHH()
fourfive_days_ago = fourtyfive_ago_YYYYMMDDHH()




countries = ['DE','GB']

for c in countries:

    #=================================
    # Copy data from CDW
    #=================================
    print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Copying data from CDW to UDW...")

    c_lower = c.lower()

    # TABLES:
    # udw_prod.UDW_CLIENTSOLUTIONS_CS.cdw_de_alexa_last_45_registers(psid VARCHAR(556));
    # udw_prod.UDW_CLIENTSOLUTIONS_CS.cdw_gb_alexa_last_45_registers(psid VARCHAR(556));

    # get registrations from last 45 days from CDW
    query_eu_last_45_registers = f'''
        DROP TABLE IF EXISTS last_45_registers;
        CREATE TEMP TABLE last_45_registers AS (
            SELECT DISTINCT  psid 
            FROM data_kpi_src.fact_voice t
            WHERE 
                partition_country = '{c}'
                AND category IN ('EV110')
                AND payload_exe_goal IN ('LOGIN_ACTIVATETV')
                AND payload_appid = 'com.samsung.tv.alexa-client'
                AND partition_date BETWEEN '{fourfive_days_ago}' AND '{yesterday_end}'
        );

        SELECT * FROM last_45_registers;
    '''

    # capture last 45 day registrations from CDW
    df_last_45_registers = pd.read_sql(query_eu_last_45_registers, con = cdw_conn)

    # QUIRK!!! columns must be uppercase or error is thrown
    df_last_45_registers.columns = map(lambda x: str(x).upper(), df_last_45_registers.columns)

    # write last 45 day registrations to UDW
    write_pandas(
        conn        = udw_conn,                             # db connection
        df          = df_last_45_registers,                 # pandas dataframe (data)
        table_name  = f'CDW_{c}_ALEXA_LAST_45_REGISTERS',   # table to copy data into
        schema      = 'UDW_CLIENTSOLUTIONS_CS',             # schema to use
        overwrite   = True                                  # append data to table or overwrite it?
    )


    # TABLES:
    # udw_prod.UDW_CLIENTSOLUTIONS_CS.cdw_de_alexa_current_day(psid VARCHAR(556));
    # udw_prod.UDW_CLIENTSOLUTIONS_CS.cdw_gb_alexa_current_day(psid VARCHAR(556));

    # get registrations from yesterday from CDW
    query_eu_current_day = f'''
        DROP TABLE IF EXISTS current_day;
        CREATE TEMP TABLE current_day AS (
            SELECT DISTINCT psid 
            FROM data_kpi_src.fact_voice t
            WHERE 
                partition_country = '{c}'
                AND category IN ('EV110')
                AND payload_exe_goal IN ('LOGIN_ACTIVATETV')
                AND payload_appid = 'com.samsung.tv.alexa-client'
                AND partition_date BETWEEN '{yesterday_start}' AND '{yesterday_end}'
        );

        SELECT * FROM current_day;
    '''

    # capture current_day registrations from CDW
    df_current_day = pd.read_sql(query_eu_current_day, con = cdw_conn)

    # QUIRK!!! columns must be uppercase or error is thrown
    df_current_day.columns = map(lambda x: str(x).upper(), df_current_day.columns)

    # write current_day registrations to UDW
    write_pandas(
        conn        = udw_conn,                         # db connection
        df          = df_current_day,                   # pandas dataframe (data)
        table_name  = f'CDW_{c}_ALEXA_CURRENT_DAY',     # table to copy data into
        schema      = 'UDW_CLIENTSOLUTIONS_CS',         # schema to use
        overwrite   = True                              # append data to table or overwrite it?
    )


    #=================================
    # Build audience in UDW
    #=================================
    print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Building audiences...")

    # create a 10% control from current_day registrations
    query_udw_1 = f'''
        DROP TABLE IF EXISTS aa_seg_100;
    '''

    query_udw_1b = f'''
        CREATE TEMP TABLE aa_seg_100 AS (
            SELECT
                psid,
                NTILE(100) OVER (ORDER BY RANDOM()) rownum
            FROM udw_prod.udw_clientsolutions_cs.cdw_{c}_alexa_current_day
        );
    '''

    udw_cur.execute(query_udw_1)
    udw_cur.execute(query_udw_1b)


    query_udw_2 = f'''
        DROP TABLE IF EXISTS control;
    '''

    query_udw_2b = f'''
        CREATE TEMP TABLE control AS (
            SELECT DISTINCT
                psid
            FROM aa_seg_100
            WHERE
                rownum <= 10 
        );
    '''

    udw_cur.execute(query_udw_2)
    udw_cur.execute(query_udw_2b)


    # the "current control" is the previous "new control"
    # import previous "new control" from UDW
    query_udw_3 = f'''
        DROP TABLE IF EXISTS current_control;
    '''

    query_udw_3b = f'''
        CREATE TEMP TABLE current_control AS (
            SELECT 
                psid
            FROM udw_prod.udw_clientsolutions_cs.cdw_{c}_alexa_new_control
        );
    '''

    udw_cur.execute(query_udw_3)
    udw_cur.execute(query_udw_3b)


    # generate the new control
    # later we will save the "new control" to s3 and UDW
    # new control = DISTINCT (this control + previous control) 
    query_udw_4 = f'''
        DROP TABLE IF EXISTS new_control;
    '''

    query_udw_4b = f'''
        CREATE TEMP TABLE new_control AS (
            SELECT 
                psid
            FROM control
            UNION
            SELECT 
                psid
            FROM current_control
        );
    '''

    udw_cur.execute(query_udw_4)
    udw_cur.execute(query_udw_4b)


    # "new segment" = last 45 day registrations - new control
    # later we will save the "new segment" to s3 and UDW
    query_udw_5 = f'''
        DROP TABLE IF EXISTS new_seg;
    '''

    query_udw_5b = f'''
        CREATE TEMP TABLE new_seg AS (
            SELECT 
                psid
            FROM udw_prod.udw_clientsolutions_cs.cdw_{c}_alexa_last_45_registers
            WHERE 
                psid NOT IN (
                    SELECT 
                        psid
                    FROM new_control
                )
        );
    '''

    udw_cur.execute(query_udw_5)
    udw_cur.execute(query_udw_5b)


    # save new_control to UDW
    query_udw_6 = f'''
        CREATE OR REPLACE TABLE udw_prod.udw_clientsolutions_cs.cdw_{c}_alexa_new_control AS (
            SELECT psid 
            FROM new_control
        );
    '''

    udw_cur.execute(query_udw_6)


    # save new segment to UDW
    query_udw_7 = f'''
        CREATE OR REPLACE TABLE udw_prod.udw_clientsolutions_cs.cdw_{c}_alexa_new_segment AS (
            SELECT psid 
            FROM new_seg
        );
    '''

    udw_cur.execute(query_udw_7)



    #=================================
    # save new control and new segment to s3
    #=================================
    # Stage: AUDIENCE_PLANNER_REMOTE_FILES_UDW_S
    # SQL to Check in DbVis: LIST @UDW_PROD.UDW_CLIENTSOLUTIONS_CS.audience_planner_remote_files_udw_s PATTERN = '.*230912_.*';

    print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Saving data to s3...")

    # saving segment
    query_udw_100 = f'''
        COPY INTO  @UDW_PROD.UDW_CLIENTSOLUTIONS_CS.audience_planner_remote_files_udw_s/230912_{c_lower}_alexa_register_last_45_230912to231231.csv
        FROM (SELECT DISTINCT psid FROM new_seg)
        file_format = (format_name = adbiz_data.analytics_csv COMPRESSION = 'none')
        single = TRUE
        header = TRUE
        overwrite = TRUE
        max_file_size = 4900000000; 
    '''

    udw_cur.execute(query_udw_100)


    # saving control
    query_udw_200 = f'''
        COPY INTO  @UDW_PROD.UDW_CLIENTSOLUTIONS_CS.audience_planner_remote_files_udw_s/220601_{c_lower}_alexa_10per_control.csv
        FROM (SELECT DISTINCT psid FROM new_control)
        file_format = (format_name = adbiz_data.analytics_csv COMPRESSION = 'none')
        single = TRUE
        header = TRUE
        overwrite = TRUE
        max_file_size = 4900000000; 
    '''

    udw_cur.execute(query_udw_200)


#================================================================
# START CLOSE DB CONNECTIONS
#================================================================
udw_cur.close()
udw_conn.close()

cdw_cur.close()
cdw_conn.close()

#================================================================
# END CLOSE DB CONNECTIONS
#================================================================


print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Task done. All connections disconnected.")
end_timestamp = datetime.now()
time_used = end_timestamp - start_timestamp


print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Total running time: ", str(time_used).split('.')[0])


