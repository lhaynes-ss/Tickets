
import custom_global_app_use as ap
import requests
import json
from datetime import datetime

'''
TODO: Add MX?
'''


# set script vars
destination_table       = 'udw_clientsolutions_cs.pluto_custom_app_usage'
udw_config              = 'serviceAccount'
app_name                = 'Pluto TV'

# notification sent to slack on failure
slack_endpoint = 'https://hooks.slack.com/triggers/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'


try:
    
    # -------------------------------------------------------------------------------
    # set instance vars case 1
    # -------------------------------------------------------------------------------
    cdw_config_1        = 'personalAccountEU'
    countries_1         = 'AT, DE, ES, FR, GB, IT'
    region_1            = 'cdw_eu'
        
    ap.get_app_usage_from_cdw_to_udw(destination_table,cdw_config_1, udw_config, countries_1, region_1, app_name)
    
    # -------------------------------------------------------------------------------
    # set instance vars case 2
    # -------------------------------------------------------------------------------
    cdw_config_2        = 'personalAccountEU'
    countries_2         = 'DK, NO, SE'
    region_2            = 'cdw_nordics'
        
    ap.get_app_usage_from_cdw_to_udw(destination_table,cdw_config_2, udw_config, countries_2, region_2, app_name)
    
    # -------------------------------------------------------------------------------
    # set instance vars case 3
    # -------------------------------------------------------------------------------
    cdw_config_3        = 'personalAccountAPAC'
    countries_3         = 'AU'
    region_3            = 'cdw_apac'
        
    ap.get_app_usage_from_cdw_to_udw(destination_table,cdw_config_3, udw_config, countries_3, region_3, app_name)
    
    # -------------------------------------------------------------------------------
    # set instance vars case 4
    # -------------------------------------------------------------------------------
    cdw_config_4        = 'personalAccountSA'
    countries_4         = 'BR'
    region_4            = 'cdw_sa'
        
    ap.get_app_usage_from_cdw_to_udw(destination_table,cdw_config_4, udw_config, countries_4, region_4, app_name)


except Exception as e:
    
    # create json
    # message contains 'Task name/filename' failed
    url = slack_endpoint
    headers = {'Content-type': 'application/json'}
    data = {
        "date": f"{datetime.now().strftime("%Y-%m-%d %H:%M")}",
        "message": f"Local Python task 'Pluto Auto App Usage Data/pluto_report_app_usage' failed. Please review the windows task. {e}",
        "process_name": "Local Python Task Monitor"
    }

    response = requests.post(url, data=json.dumps(data), headers=headers)

    print(response.status_code)
    print(response.content)
    
