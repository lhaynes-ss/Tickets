'''
===================================================
IMPORT EXPOSURE DATA FROM CDW TO UDW
===================================================
imports app usage data for global regions from CDW to UDW.

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
import os


#vars
'''
DEFINITION:
------------
destination_table   = name of table that app usage data will be written to
cdw_config          = config property used for CDW connection
udw_config          = config property used for CDW connection
countries           = coma separated list of 2-digit country codes
region              = key which indicates which db and region we are reporting on. CDW and EU, SA, APAC, NORDICS
app_name            = the name of the app we are collecting usage data for 

EXAMPLES:
-----------
destination_table   = 'udw_clientsolutions_cs.paramount_custom_app_usage'
cdw_config          = 'personalAccountEU'
udw_config          = 'serviceAccount'
countries           = 'DE, FR, IT'
region              = 'cdw_eu' or 'cdw_nordics'
app_name            = 'Pluto TV'

'''


def get_app_usage_from_cdw_to_udw(destination_table, cdw_config, udw_config, countries, region, app_name):
    
    # get config
    current_path    = os.path.dirname(os.path.abspath(__file__))
    config          = configparser.ConfigParser()
    config_path     = Path(fr'{current_path}\config.ini')
    config.read(config_path)

    
    # output start time
    start_timestamp = datetime.now()
    print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Opening connections...")
    
    
    #================================================================
    # START OPEN DB CONNECTIONS
    #================================================================

    try: 
        
        # connect to CDW
        cdw_conn = psycopg2.connect(
            dbname = config[cdw_config]["dbname"],
            host = config[cdw_config]["host"],
            port = config[cdw_config]["port"],
            user = config[cdw_config]["user"],
            password = config[cdw_config]["password"]
        )

        cdw_cur = cdw_conn.cursor()
        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> CDW connected")


        # connect to UDW
        udw_conn = snowflake.connector.connect(
            user = config[udw_config ]["user"],
            password = config[udw_config ]["password"],
            account = config[udw_config ]["account"],
            warehouse = config[udw_config ]["warehouse"],
            database = config[udw_config ]["database"],
            schema = config[udw_config ]["schema"]
        )

        udw_cur = udw_conn.cursor()
        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> UDW NA connected.")


        #================================================================
        # END OPEN DB CONNECTIONS
        #================================================================

        #================================================================
        # START- PULL CDW APP USAGE DATA
        #================================================================
        
        # workaround to split countries list up into a table in redshift as there 
        # is no split-to-table function in redshift.
        # this implementation can accept up to 20 values in the list
        cdw_country_query = f'''
            DROP TABLE IF EXISTS countries_table;
            CREATE TEMP TABLE countries_table AS (
                
                WITH sequence_cte AS (
                    SELECT 1 AS num UNION ALL
                    SELECT 2 UNION ALL
                    SELECT 3 UNION ALL
                    SELECT 4 UNION ALL
                    SELECT 5 UNION ALL
                    SELECT 6 UNION ALL
                    SELECT 7 UNION ALL
                    SELECT 8 UNION ALL
                    SELECT 9 UNION ALL
                    SELECT 10 UNION ALL
                    SELECT 11 UNION ALL
                    SELECT 12 UNION ALL
                    SELECT 13 UNION ALL
                    SELECT 14 UNION ALL
                    SELECT 15 UNION ALL
                    SELECT 16 UNION ALL
                    SELECT 17 UNION ALL
                    SELECT 18 UNION ALL
                    SELECT 19 UNION ALL
                    SELECT 20
                )

                SELECT TRIM(SPLIT_PART('{countries}', ',', s.num)) AS country
                FROM sequence_cte s
                WHERE 
                    country <> ''
                    
            );
        '''

        cdw_cur.execute(cdw_country_query)
        
        
        # set dates in temp table as CDW does not have variables
        # script updated to go back 6 months instead of x days on initial load, then 2 weeks going forward
        #---------------------------------------------------------------------------------
        #--------------
        # production
        #--------------
        cdw_variable_query = f'''
            DROP TABLE IF EXISTS variable_table;
            CREATE TEMP TABLE variable_table AS (
                SELECT 
                    DATEADD('week', -2, CURRENT_DATE)::TIMESTAMP AS reporting_start
                    ,(LEFT(DATEADD('day', -1, CURRENT_DATE), 10)::VARCHAR || ' 23:59:59')::TIMESTAMP AS reporting_end  
                    ,DATEADD('week', -2, CURRENT_DATE)::TIMESTAMP AS quarter_start  
                    -- ,DATE_TRUNC('quarter', CURRENT_DATE)::TIMESTAMP AS quarter_start
            );
        '''
        
        #--------------
        # test
        #--------------
        # cdw_variable_query = f'''
        #     DROP TABLE IF EXISTS variable_table;
        #     CREATE TEMP TABLE variable_table AS (
        #         SELECT 
        #             '2023-11-01 00:00:00'::TIMESTAMP AS reporting_start
        #             ,'2023-11-01 00:00:00'::TIMESTAMP AS quarter_start
        #             ,'2024-04-30 23:59:59'::TIMESTAMP AS reporting_end  
        #     );
        # '''

        cdw_cur.execute(cdw_variable_query)
        
        #---------------------------------------------------------------------------------


        # query for app usage depending on region specified
        if region in ('cdw_apac', 'cdw_eu', 'cdw_sa'):
            
            # get app data for EU, AU, SA
            cdw_app_query = f'''
                SELECT 
                    psid_tvid(f.psid) AS tifa
                    ,f.start_timestamp AS app_usage_datetime
                    ,f.country
                    ,f.app_id
                    ,SUM(DATEDIFF('minutes', f.start_timestamp, f.end_timestamp)) AS time_spent_min
                    ,COUNT(*) AS usage_count
                    ,CURRENT_DATE AS date_imported
                FROM data_tv_acr.fact_app_usage_session f
                    JOIN variable_table v ON 1 = 1
                WHERE 
                    f.app_id IN (
                        SELECT DISTINCT app_id 
                        FROM meta_apps.meta_taps_sra_app_lang_l 
                        WHERE prod_nm = '{app_name}'
                    ) 
                    AND f.country IN (SELECT DISTINCT ct.country FROM countries_table ct)
                    AND DATEDIFF('second', f.start_timestamp, f.end_timestamp) >= 60
                    AND f.partition_datehour BETWEEN 
                        (TO_CHAR(CAST(LEAST(v.quarter_start, v.reporting_start) AS DATE), 'yyyymmdd') || '00') 
                        AND (TO_CHAR(CAST(v.reporting_end AS DATE), 'yyyymmdd') || '23')
                GROUP BY 1, 2, 3, 4, 7
                ;
            '''
            
        elif region in ('cdw_nordics'):
            
            # get app data for nordics
            # a different table is used for nordics and no time duration data is available
            # so we need a different query for notdics.
            cdw_app_query = f'''
                SELECT 
                    psid_tvid(f.psid) AS tifa 
                    ,f.event_time AS app_usage_datetime
                    ,f.country
                    ,f.app_id
                    ,0 AS time_spent_min    --> time usage not available for nordics
                    ,COUNT(*) AS usage_count
                    ,CURRENT_DATE AS date_imported
                FROM data_tv_smarthub.fact_app_opened_event f
                    JOIN variable_table v ON 1 = 1
                WHERE 
                    f.app_id IN (
                        SELECT DISTINCT app_id::VARCHAR 
                        FROM meta_apps.meta_taps_sra_app_lang_l 
                        WHERE prod_nm = '{app_name}'
                    ) 
                    AND f.country IN (SELECT DISTINCT ct.country FROM countries_table ct)
                    AND f.partition_date BETWEEN 
                        (TO_CHAR(CAST(LEAST(v.quarter_start, v.reporting_start) AS DATE), 'yyyymmdd')) 
                        AND (TO_CHAR(CAST(v.reporting_end AS DATE), 'yyyymmdd'))
                GROUP BY 1, 2, 3, 4, 7
                ;
            '''
            
        else:
            
            # if we are here, and invalid region was specified
            raise Exception("Invalid region supplied.")
            
            
        # store results of app usage query in a dataframe
        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> CDW getting app usage...")
        cdw_app_use_df = pd.read_sql(cdw_app_query, con = cdw_conn)
        
        
        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> CDW preparing app usage data...")
        
        # specify data type for fields as these may be different between EU, APAC, and SA
        if len(cdw_app_use_df) > 0:    
            print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Casting data type for each column...")
            cdw_app_use_df['tifa']                  = cdw_app_use_df['tifa'].astype(str)
            cdw_app_use_df['app_usage_datetime']    = cdw_app_use_df['app_usage_datetime'].apply(pd.to_datetime).dt.strftime('%Y-%m-%d %H:%M:%S').astype(str)
            cdw_app_use_df['country']               = cdw_app_use_df['country'].astype(str)
            cdw_app_use_df['app_id']                = cdw_app_use_df['app_id'].astype(str)
            cdw_app_use_df['time_spent_min']        = cdw_app_use_df['time_spent_min'].astype(int)
            cdw_app_use_df['usage_count']           = cdw_app_use_df['usage_count'].astype(int)
            cdw_app_use_df['date_imported']         = cdw_app_use_df['date_imported'].apply(pd.to_datetime).dt.strftime('%Y-%m-%d').astype(str)
        else:
            print("No data to update!")
            
            
        # run connection hooks again for UDW. May be unnecessary
        udw_cur.execute("USE ROLE UDW_CLIENTSOLUTIONS_REPORTING_MAINTAINER_ROLE_PROD;")
        udw_cur.execute("USE DATABASE UDW_PROD;")
        udw_cur.execute("USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;")
        udw_cur.execute("USE SCHEMA UDW_CLIENTSOLUTIONS_CS;")


        # prepare to bring data into UDW by creating a temp table in UDW
        udw_import_prep_query = f'''
            CREATE TEMP TABLE APP_PREP (
                tifa VARCHAR
                ,app_usage_datetime TIMESTAMP
                ,country VARCHAR
                ,app_id VARCHAR
                ,time_spent_min BIGINT
                ,usage_count INT
                ,date_imported DATE
            );
        '''

        udw_cur.execute(udw_import_prep_query)
        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> UDW temp table created...")
        
        
        # write_pandas QUIRK!!! columns must be uppercase or error is thrown
        cdw_app_use_df.columns = map(lambda x: str(x).upper(), cdw_app_use_df.columns)

        # print(cdw_app_use_df)
        
        
        # write CDW app use data into UDW
        # write_pandas QUIRK!!! table name and schema should be uppercase
        write_pandas(
            conn            = udw_conn                      # db connection
            ,df             = cdw_app_use_df                # pandas dataframe (data)
            ,table_name     = 'APP_PREP'                    # table to copy data into
            ,schema         = 'UDW_CLIENTSOLUTIONS_CS'      # schema to use
        )

        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Data loaded into UDW temp table...")
        
        
        # insert new data (any rows not already in the UDW table)
        udw_import_query = f'''
            INSERT INTO {destination_table} (
                SELECT
                    tifa
                    ,app_usage_datetime::TIMESTAMP
                    ,country
                    ,app_id
                    ,time_spent_min
                    ,usage_count
                    ,date_imported::DATE AS date_imported
                FROM app_prep AS new_data
                WHERE 
                    NOT EXISTS (
                        SELECT 1
                        FROM {destination_table} AS old_data
                        WHERE 
                            old_data.tifa                       = new_data.tifa
                            AND old_data.app_usage_datetime     = new_data.app_usage_datetime 
                            AND old_data.country                = new_data.country
                            AND old_data.app_id                 = new_data.app_id
                            AND old_data.time_spent_min         = new_data.time_spent_min
                    )
            )
        ;
        '''

        udw_cur.execute(udw_import_query)

        #================================================================
        # END - PULL CDW EXPOSURE DATA
        #================================================================
        
        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Process completed sucessfully.")
    
    except Exception as e:
        
        # get exception as pass it back up to the calling script
        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Process failed.")
        
        raise Exception("App usage data was not able to be retrieved.") from e

    finally:
        
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
        
        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> All connections disconnected.")
        end_timestamp = datetime.now()
        time_used = end_timestamp - start_timestamp


        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Total running time: ", str(time_used).split('.')[0])
        
        
        return True






