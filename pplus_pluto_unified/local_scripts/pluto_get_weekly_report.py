'''
===================================================
PLUTO
GET WEEKLY REPORT

# specify stage values
    LIST @udw_marketing_analytics_reports.pluto_external_us/pluto-us/;
    LIST @udw_marketing_analytics_reports.pluto_external_international/pluto-international/;
    
# file name prefix
    pluto_
    
TODO: Add slack notification on failure
TODO: Add task to run file on Monday mornings
===================================================
'''

# import packages
import custom_global_exposures as ex
import requests
import json
from datetime import datetime, timedelta, date
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import configparser
from pathlib import Path
import os


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


# vars 
start_date                  = ''                        # 'YYYY-MM-DD' or if '' then dates will be auto-set by the stored proc
end_date                    = ''                        # 'YYYY-MM-DD' or if '' then dates will be auto-set by the stored proc
      
partner                     = 'pluto'                   # paramount | pluto
interval                    = 'weekly'                  # weekly | monthly
regions                     = {                         # countries by connection
                                'udw_na'        : 'US, CA'
                                ,'cdw_eu'       : 'AT, DE, ES, FR, GB, IT'
                                ,'cdw_nordics'  : 'DK, NO, SE'
                                ,'cdw_apac'     : 'AU'
                                ,'cdw_sa'       : 'BR'
                            }
max_rows                    = 999999                    # max # of rows per report. Point at which report is split into additional file. 1 million - 1 for header
attribution_window          = 7                         # max days after exposure for attribution credit

us_stage                    = '@udw_marketing_analytics_reports.pluto_external_us/pluto-us/'
int_stage                   = '@udw_marketing_analytics_reports.pluto_external_international/pluto-international/'
file_name_prefix            = 'pluto_'

attribution_window_days     = 7                         # number of days for conversion attribution
lookback_window_months      = 12                        # number of months for lookback window
page_visit_lookback_days    = 30                        # number of days for web pixel lookback
operative_table             = 'udw_prod.udw_clientsolutions_cs.pluto_operative_sales_orders'
mapping_table               = 'udw_prod.udw_clientsolutions_cs.pluto_custom_creative_mapping'
app_name                    = 'Pluto TV'                # app name
signup_segment              = ''                        # segment id for signup pixel or ''
homepage_segment            = ''                        # segment id for homepage pixel or ''


# generate query for each region
for region in regions:
    countries = regions[region]
            
    get_reprts_query = f'''
        CALL udw_prod.udw_clientsolutions_cs.sp_partner_get_weekly_reports(
            partner                     => '{partner}'
            ,region                     => '{region}'
            ,report_interval            => '{interval}'
            ,start_date                 => '{start_date}'
            ,end_date                   => '{end_date}'
            ,countries                  => '{countries}'
            ,max_rows                   => {max_rows}
            ,attribution_window         => {attribution_window}
            ,us_stage                   => '{us_stage}'
            ,int_stage                  => '{int_stage}'
            ,file_name_prefix           => '{file_name_prefix}'
            ,attribution_window_days    => {attribution_window_days}  
            ,lookback_window_months     => {lookback_window_months}   
            ,page_visit_lookback_days   => {page_visit_lookback_days} 
            ,operative_table            => '{operative_table}'        
            ,mapping_table              => '{mapping_table}'          
            ,app_name                   => '{app_name}'               
            ,signup_segment             => '{signup_segment}'         
            ,homepage_segment           => '{homepage_segment}'       
        );
    '''
    
    # print(get_reprts_query)
    
    udw_cur.execute(get_reprts_query)


# close connection
udw_cur.close()
udw_conn.close()


# Example output
'''
CALL udw_prod.udw_clientsolutions_cs.sp_partner_get_weekly_reports(
    partner                     => 'pluto'
    ,region                     => 'udw_na'
    ,report_interval            => 'weekly'
    ,start_date                 => ''
    ,end_date                   => ''
    ,countries                  => 'US, CA'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@udw_marketing_analytics_reports.pluto_external_us/pluto-us/'
    ,int_stage                  => '@udw_marketing_analytics_reports.pluto_external_international/pluto-international/'
    ,file_name_prefix           => 'pluto_'
    ,attribution_window_days    => 7  
    ,lookback_window_months     => 12   
    ,page_visit_lookback_days   => 30 
    ,operative_table            => 'udw_prod.udw_clientsolutions_cs.pluto_operative_sales_orders'        
    ,mapping_table              => 'udw_prod.udw_clientsolutions_cs.pluto_custom_creative_mapping'
    ,app_name                   => 'Pluto TV'
    ,signup_segment             => ''
    ,homepage_segment           => ''       
);


CALL udw_prod.udw_clientsolutions_cs.sp_partner_get_weekly_reports(
    partner                     => 'pluto'
    ,region                     => 'cdw_eu'
    ,report_interval            => 'weekly'
    ,start_date                 => ''
    ,end_date                   => ''
    ,countries                  => 'AT, DE, ES, FR, GB, IT'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@udw_marketing_analytics_reports.pluto_external_us/pluto-us/'
    ,int_stage                  => '@udw_marketing_analytics_reports.pluto_external_international/pluto-international/'
    ,file_name_prefix           => 'pluto_'
    ,attribution_window_days    => 7
    ,lookback_window_months     => 12
    ,page_visit_lookback_days   => 30
    ,operative_table            => 'udw_prod.udw_clientsolutions_cs.pluto_operative_sales_orders'
    ,mapping_table              => 'udw_prod.udw_clientsolutions_cs.pluto_custom_creative_mapping'
    ,app_name                   => 'Pluto TV'
    ,signup_segment             => ''
    ,homepage_segment           => ''
);


CALL udw_prod.udw_clientsolutions_cs.sp_partner_get_weekly_reports(
    partner                     => 'pluto'
    ,region                     => 'cdw_nordics'
    ,report_interval            => 'weekly'
    ,start_date                 => ''
    ,end_date                   => ''
    ,countries                  => 'DK, NO, SE'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@udw_marketing_analytics_reports.pluto_external_us/pluto-us/'
    ,int_stage                  => '@udw_marketing_analytics_reports.pluto_external_international/pluto-international/'
    ,file_name_prefix           => 'pluto_'
    ,attribution_window_days    => 7
    ,lookback_window_months     => 12
    ,page_visit_lookback_days   => 30
    ,operative_table            => 'udw_prod.udw_clientsolutions_cs.pluto_operative_sales_orders'
    ,mapping_table              => 'udw_prod.udw_clientsolutions_cs.pluto_custom_creative_mapping'
    ,app_name                   => 'Pluto TV'
    ,signup_segment             => ''
    ,homepage_segment           => ''
);


CALL udw_prod.udw_clientsolutions_cs.sp_partner_get_weekly_reports(
    partner                     => 'pluto'
    ,region                     => 'cdw_apac'
    ,report_interval            => 'weekly'
    ,start_date                 => ''
    ,end_date                   => ''
    ,countries                  => 'AU'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@udw_marketing_analytics_reports.pluto_external_us/pluto-us/'
    ,int_stage                  => '@udw_marketing_analytics_reports.pluto_external_international/pluto-international/'
    ,file_name_prefix           => 'pluto_'
    ,attribution_window_days    => 7
    ,lookback_window_months     => 12
    ,page_visit_lookback_days   => 30
    ,operative_table            => 'udw_prod.udw_clientsolutions_cs.pluto_operative_sales_orders'
    ,mapping_table              => 'udw_prod.udw_clientsolutions_cs.pluto_custom_creative_mapping'
    ,app_name                   => 'Pluto TV'
    ,signup_segment             => ''
    ,homepage_segment           => ''
);


CALL udw_prod.udw_clientsolutions_cs.sp_partner_get_weekly_reports(
    partner                     => 'pluto'
    ,region                     => 'cdw_sa'
    ,report_interval            => 'weekly'
    ,start_date                 => ''
    ,end_date                   => ''
    ,countries                  => 'BR'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@udw_marketing_analytics_reports.pluto_external_us/pluto-us/'
    ,int_stage                  => '@udw_marketing_analytics_reports.pluto_external_international/pluto-international/'
    ,file_name_prefix           => 'pluto_'
    ,attribution_window_days    => 7
    ,lookback_window_months     => 12
    ,page_visit_lookback_days   => 30
    ,operative_table            => 'udw_prod.udw_clientsolutions_cs.pluto_operative_sales_orders'
    ,mapping_table              => 'udw_prod.udw_clientsolutions_cs.pluto_custom_creative_mapping'
    ,app_name                   => 'Pluto TV'
    ,signup_segment             => ''
    ,homepage_segment           => ''
);
'''






