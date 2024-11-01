'''
===================================================
PLUTO
IMPORT EXPOSURE DATA FROM CDW TO UDW

Databases:
    CDW EU
    CDW APAC
    CDW SA
    UDW_PROD

Source Tables:
    cdw.data_ad_xdevice.fact_delivery_event
    udw_clientsolutions_cs.pluto_operative_sales_orders
    
Destination Tables:
    udw_clientsolutions_cs.pluto_custom_global_exposure
    
===================================================
'''

# import packages
import custom_global_exposures as ex
import requests
import json
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import configparser
from pathlib import Path
import os

    
# notification sent to slack on failure
slack_endpoint = 'https://hooks.slack.com/triggers/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'


try:

    # get config
    current_path    = os.path.dirname(os.path.abspath(__file__))
    config          = configparser.ConfigParser()
    config_path     = Path(fr'{current_path}\config.ini')
    config.read(config_path)


    # connect to UDW
    udw_config = 'serviceAccount'

    udw_conn = snowflake.connector.connect(
        user = config[udw_config ]["user"],
        password = config[udw_config ]["password"],
        account = config[udw_config ]["account"],
        warehouse = config[udw_config ]["warehouse"],
        database = config[udw_config ]["database"],
        schema = config[udw_config ]["schema"]
    )

    udw_cur = udw_conn.cursor()

    #=============
    # UDW
    #=============
    # run connection hooks
    udw_cur.execute("USE ROLE UDW_CLIENTSOLUTIONS_REPORTING_MAINTAINER_ROLE_PROD;")
    udw_cur.execute("USE DATABASE UDW_PROD;")
    udw_cur.execute("USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;")
    udw_cur.execute("USE SCHEMA UDW_CLIENTSOLUTIONS_CS;")


    # pluto sales orders
    # 13191 --> Pluto TV - Intl
    # 13190 --> Pluto TV - US
    update_sales_order_query = f'''
        CALL udw_clientsolutions_cs.sp_update_custom_operative_sales_orders(
            advertiser_ids => '13191, 13190'
            ,destination_table => 'udw_clientsolutions_cs.pluto_operative_sales_orders'
        );
    '''

    udw_cur.execute(update_sales_order_query)

            
    #=============
    # pluto+
    #=============
    #vars EU
    sales_order_table_1   = 'udw_clientsolutions_cs.pluto_operative_sales_orders'
    destination_table_1   = 'udw_clientsolutions_cs.pluto_custom_global_exposure'
    cdw_config_1          = 'personalAccountEU'
    udw_config_1          = 'serviceAccount'

    ex.get_exposures_from_cdw_to_udw(sales_order_table_1, destination_table_1, cdw_config_1, udw_config_1)



    #vars APAC
    sales_order_table_2   = 'udw_clientsolutions_cs.pluto_operative_sales_orders'
    destination_table_2   = 'udw_clientsolutions_cs.pluto_custom_global_exposure'
    cdw_config_2          = 'personalAccountAPAC'
    udw_config_2          = 'serviceAccount'

    ex.get_exposures_from_cdw_to_udw(sales_order_table_2, destination_table_2, cdw_config_2, udw_config_2)



    #vars SA
    sales_order_table_3   = 'udw_clientsolutions_cs.pluto_operative_sales_orders'
    destination_table_3   = 'udw_clientsolutions_cs.pluto_custom_global_exposure'
    cdw_config_3          = 'personalAccountSA'
    udw_config_3          = 'serviceAccount'

    ex.get_exposures_from_cdw_to_udw(sales_order_table_3, destination_table_3, cdw_config_3, udw_config_3)
    

except Exception as e:
    
    # create json
    # message contains 'Task name/filename' failed
    url = slack_endpoint
    headers = {'Content-type': 'application/json'}
    data = {
        "date": f"{datetime.now().strftime("%Y-%m-%d %H:%M")}",
        "message": f"Local Python task 'Pluto Auto Mapping Data/pluto_report_exposures' failed. Please review the windows task. {e}",
        "process_name": "Local Python Task Monitor"
    }

    response = requests.post(url, data=json.dumps(data), headers=headers)

    print(response.status_code)
    print(response.content)

finally:
    
    # close connection
    udw_cur.close()
    udw_conn.close()

