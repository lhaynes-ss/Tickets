'''
===================================================
IMPORT EXPOSURE DATA FROM CDW TO UDW
===================================================
imports exposure data for global regions from CDW to UDW.

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
map_table           = name of table that contains mapping data
destination_table   = name of table that expoure data will be written to
cdw_config          = config property used for CDW connection
udw_config          = config property used for CDW connection

EXAMPLE:
-----------
map_table           = 'udw_clientsolutions_cs.paramount_custom_creative_mapping'
destination_table   = 'udw_clientsolutions_cs.paramount_custom_global_exposure'
cdw_config          = 'personalAccountEU'
udw_config          = 'serviceAccount'
'''


def get_exposures_from_cdw_to_udw(sales_order_table, destination_table, cdw_config, udw_config):
    
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


        #=============
        # UDW
        #=============
        # run connection hooks
        udw_cur.execute("USE ROLE UDW_CLIENTSOLUTIONS_REPORTING_MAINTAINER_ROLE_PROD;")
        udw_cur.execute("USE DATABASE UDW_PROD;")
        udw_cur.execute("USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;")
        udw_cur.execute("USE SCHEMA UDW_CLIENTSOLUTIONS_CS;")
        
        
        # set UDW dates for mapping query
        # script updated to go back 6 months instead of x days on initial load, then 1 month going forward
        # udw_cur.execute("SET reporting_start = (SELECT DATEADD('month', -6, CURRENT_DATE)::TIMESTAMP);") # initial load
        udw_cur.execute("SET reporting_start = (SELECT DATEADD('month', -1, CURRENT_DATE)::TIMESTAMP);")
        udw_cur.execute("SET reporting_end   = (SELECT (DATEADD('day', -1, CURRENT_DATE)::VARCHAR || ' 23:59:59')::TIMESTAMP);")
        udw_cur.execute("SET quarter_start   = (SELECT DATE_TRUNC('quarter', CURRENT_DATE)::TIMESTAMP);")

        
        # generate creative map from UDW by combining sales order and campaign info
        # load into a dataframe
        get_map_query1 = f'''
            CREATE TEMP TABLE cmpgn AS (
                
                -- get campaign data for active campaigns
                SELECT DISTINCT
                    oms_att.sales_order_id
                    ,cmpgn.id AS campaign_id
                    ,cmpgn.name AS campaign_name
                    ,oms_att.package_sales_order_line_item_id
                FROM trader.campaigns_latest AS cmpgn
                    JOIN (
                        SELECT DISTINCT
                            cmpgn_att.campaign_id
                            ,cmpgn_att.io_external_id AS sales_order_id
                            ,cmpgn_att.li_external_id AS package_sales_order_line_item_id
                        FROM trader.campaign_oms_attrs_latest AS cmpgn_att
                    ) AS oms_att ON cmpgn.id = oms_att.campaign_id
                WHERE 
                    cmpgn.state != 'archived'
            );
        '''
        
        get_map_query2 = f'''
            SELECT DISTINCT
                so.vao
                ,c.campaign_id
            FROM TABLE('{sales_order_table}') so
                JOIN cmpgn c ON c.package_sales_order_line_item_id = so.package_sales_order_line_item_id
            WHERE 
                1 = 1
                AND so.vao IS NOT NULL
                AND so.sales_order_name IS NOT NULL
                AND so.sales_order_name != ''
                AND c.campaign_id IS NOT NULL
                AND so.package_sales_order_line_item_end_at >= LEAST($reporting_start, $quarter_start)
                AND so.package_sales_order_line_item_start_at <= $reporting_end
            ;
        '''

        udw_cur.execute(get_map_query1)
        udw_df_map = pd.read_sql(get_map_query2, con = udw_conn)

        
        
        #=============
        # CDW
        #=============
        # change map column names to lower case for CDW
        udw_df_map.columns = [c.lower() for c in udw_df_map.columns]


        # specify data type for fields as these may be different between EU, APAC, and SA
        udw_df_map['vao'] = udw_df_map['vao'].astype('Int64')
        udw_df_map['campaign_id'] = udw_df_map['campaign_id'].astype('Int64')
        
        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> UDW data aggregated. Casting data type for each column...")


        # create a temp table in CDW to hold data from UDW
        cdw_import_prep_query = f'''
            CREATE TEMP TABLE exp_prep (
                vao             INT
                ,campaign_id    INT
            );
        '''

        cdw_cur.execute(cdw_import_prep_query)


        # import map data into UDW temp table
        for row in [tuple(array_list) for array_list in udw_df_map.values.tolist()]:
            
            format_str = '''
                INSERT INTO exp_prep (
                    vao
                    ,campaign_id
                )
                VALUES (
                    '{vao}',
                    '{campaign_id}'
                );
            '''
            
            sql_command = format_str.format(
                vao         = row[0]
                ,campaign_id = row[1]
            )
            
            cdw_cur.execute(sql_command)



        #================================================================
        # START- PULL CDW EXPOSURE DATA
        #================================================================
        
        # set dates in temp table as CDW does not have variables
        # script updated to go back 6 months instead of x days on initial load, then 1 month going forward
        cdw_variable_query = f'''
            DROP TABLE IF EXISTS variable_table;
            CREATE TEMP TABLE variable_table AS (
                SELECT 
                    DATEADD('month', -6, CURRENT_DATE)::TIMESTAMP AS reporting_start
                    ,(LEFT(DATEADD('day', -1, CURRENT_DATE), 10)::VARCHAR || ' 23:59:59')::TIMESTAMP AS reporting_end  
                    ,DATE_TRUNC('quarter', CURRENT_DATE)::TIMESTAMP AS quarter_start  
            );
        '''

        cdw_cur.execute(cdw_variable_query)


        # get exposure fact data with help of variables and map tables
        cdw_get_exposure_query = f'''
            SELECT
                m.vao
                ,f.samsung_tvid AS vtifa
                ,f.event_time AS exposure_datetime
                ,f.device_country AS country
                ,f.campaign_id
                ,f.creative_id
                ,f.flight_id
                ,f.type
            FROM data_ad_xdevice.fact_delivery_event f
                JOIN exp_prep m ON m.campaign_id = f.campaign_id
                JOIN variable_table v ON 1 = 1
            WHERE 
                1 = 1
                AND f.type IN (
                    1       --> impressions
                    ,2      --> clicks
                )
                AND (f.dropped != TRUE OR f.dropped IS NULL)
                AND f.event_time BETWEEN LEAST(v.quarter_start, v.reporting_start) AND v.reporting_end
            ;
        '''

        cdw_exposure_df = pd.read_sql(cdw_get_exposure_query, con = cdw_conn)

        # specify data type for fields as these may be different between EU, APAC, and SA
        if len(cdw_exposure_df) > 0:    
            print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> CDW data aggregated. Casting data type for each column...")
            cdw_exposure_df['vao']                  = cdw_exposure_df['vao'].astype(int)
            cdw_exposure_df['vtifa']                = cdw_exposure_df['vtifa'].astype(str)
            cdw_exposure_df['exposure_datetime']    = cdw_exposure_df['exposure_datetime'].apply(pd.to_datetime).dt.strftime('%Y-%m-%d %H:%M:%S').astype(str)
            cdw_exposure_df['country']              = cdw_exposure_df['country'].astype(str)
            cdw_exposure_df['campaign_id']          = cdw_exposure_df['campaign_id'].astype(int)
            cdw_exposure_df['creative_id']          = cdw_exposure_df['creative_id'].astype(int)
            cdw_exposure_df['flight_id']            = cdw_exposure_df['flight_id'].astype(int)
            cdw_exposure_df['type']                 = cdw_exposure_df['type'].astype(int)
        else:
            print("No data to update!")
            

        # run connection hooks again. May be unnecessary
        udw_cur.execute("USE ROLE UDW_CLIENTSOLUTIONS_REPORTING_MAINTAINER_ROLE_PROD;")
        udw_cur.execute("USE DATABASE UDW_PROD;")
        udw_cur.execute("USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;")
        udw_cur.execute("USE SCHEMA UDW_CLIENTSOLUTIONS_CS;")


        # prepare to bring back into UDW by creating a temp table in UDW
        udw_import_prep_query = f'''
            CREATE TEMP TABLE EXP_PREP (
                vao                     INT
                ,vtifa                  VARCHAR
                ,exposure_datetime      TIMESTAMP
                ,country                VARCHAR(8)
                ,campaign_id            INT
                ,creative_id            INT
                ,flight_id              INT
                ,type                   INT
            );
        '''

        udw_cur.execute(udw_import_prep_query)
        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> UDW temp table created...")


        # write_pandas QUIRK!!! columns must be uppercase or error is thrown
        cdw_exposure_df.columns = map(lambda x: str(x).upper(), cdw_exposure_df.columns)


        # write CDW exposure data into UDW
        # write_pandas QUIRK!!! table name and schema should be uppercase
        write_pandas(
            conn            = udw_conn                      # db connection
            ,df             = cdw_exposure_df               # pandas dataframe (data)
            ,table_name     = 'EXP_PREP'                    # table to copy data into
            ,schema         = 'UDW_CLIENTSOLUTIONS_CS'      # schema to use
        )

        print(datetime.now().strftime("%Y-%m-%d %H:%M"), "\n", "---> Data loaded into UDW temp table...")



        # insert new data (any rows not already in the UDW table)
        udw_import_query = f'''
            INSERT INTO {destination_table} (
                SELECT
                    vao
                    ,vtifa
                    ,exposure_datetime
                    ,country
                    ,campaign_id
                    ,creative_id
                    ,flight_id
                    ,type
                    ,CURRENT_DATE::DATE AS date_imported
                FROM exp_prep AS new_data
                WHERE 
                    NOT EXISTS (
                        SELECT 1
                        FROM {destination_table} AS old_data
                        WHERE 
                            old_data.vao                    = new_data.vao
                            AND old_data.vtifa              = new_data.vtifa 
                            AND old_data.exposure_datetime  = new_data.exposure_datetime
                            AND old_data.country            = new_data.country
                            AND old_data.campaign_id        = new_data.campaign_id
                            AND old_data.creative_id        = new_data.creative_id
                            AND old_data.flight_id          = new_data.flight_id
                            AND old_data.type               = new_data.type
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
        
        raise Exception("Exposure data was not able to be retrieved.") from e
        
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

