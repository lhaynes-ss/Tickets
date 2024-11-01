
-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- PARAMOUNT NA
CALL udw_clientsolutions_cs.sp_partner_get_weekly_reports(
    partner                     => 'paramount'
    ,region                     => 'udw_na'
    ,report_interval            => 'weekly'
    ,start_date                 => '2024-08-26'         --> if '' default dates provided by sp
    ,end_date                   => '2024-09-01'         --> if '' default dates provided by sp
    ,countries                  => 'US'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@udw_marketing_analytics_reports.paramount_plus_external_us/'
    ,int_stage                  => '@udw_marketing_analytics_reports.paramount_plus_external_international/'
    ,file_name_prefix           => 'paramount_plus_'
    ,attribution_window_days    => 7
    ,lookback_window_months     => 12
    ,page_visit_lookback_days   => 30
    ,operative_table            => 'udw_clientsolutions_cs.paramount_operative_sales_orders'
    ,mapping_table              => 'udw_clientsolutions_cs.paramount_custom_creative_mapping'
    ,app_name                   => 'Paramount+'
    ,signup_segment             => '52832'              --> set to '' if not applicable
    ,homepage_segment           => '52833'              --> set to '' if not applicable
);


-- PLUTO NA
CALL udw_clientsolutions_cs.sp_partner_get_weekly_reports(
    partner                     => 'pluto'
    ,region                     => 'udw_na'
    ,report_interval            => 'weekly'
    ,start_date                 => '2024-08-26'         --> if '' default dates provided by sp
    ,end_date                   => '2024-09-01'         --> if '' default dates provided by sp
    ,countries                  => 'US'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@udw_marketing_analytics_reports.pluto_external_us/'
    ,int_stage                  => '@udw_marketing_analytics_reports.pluto_external_international/'
    ,file_name_prefix           => 'pluto_'
    ,attribution_window_days    => 7
    ,lookback_window_months     => 12
    ,page_visit_lookback_days   => 30
    ,operative_table            => 'udw_clientsolutions_cs.pluto_operative_sales_orders'
    ,mapping_table              => 'udw_clientsolutions_cs.pluto_custom_creative_mapping'
    ,app_name                   => 'Pluto TV'
    ,signup_segment             => ''                   --> set to '' if not applicable
    ,homepage_segment           => ''                   --> set to '' if not applicable
);


--=============================================================================================================
--=============================================================================================================


-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- PARAMOUNT GLOBAL
CALL udw_clientsolutions_cs.sp_partner_get_cdw_weekly_reports(
    partner                     => 'paramount'
    ,region                     => 'cdw_eu'
    ,report_interval            => 'weekly'
    ,start_date                 => '2024-09-23'         --> if '' default dates provided by sp
    ,end_date                   => '2024-09-29'         --> if '' default dates provided by sp
    ,countries                  => 'GB'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@udw_marketing_analytics_reports.paramount_plus_external_us/'
    ,int_stage                  => '@udw_marketing_analytics_reports.paramount_plus_external_international/'
    ,file_name_prefix           => 'paramount_plus_'
    ,attribution_window_days    => 7
    ,lookback_window_months     => 12
    ,page_visit_lookback_days   => 30
    ,operative_table            => 'udw_clientsolutions_cs.paramount_operative_sales_orders'
    ,mapping_table              => 'udw_clientsolutions_cs.paramount_custom_creative_mapping'
    ,exposure_table             => 'udw_clientsolutions_cs.paramount_custom_global_exposure'
    ,app_usage_table            => 'udw_clientsolutions_cs.paramount_custom_app_usage'
    ,app_name                   => 'Paramount+'
    ,signup_segment             => ''                   --> set to '' if not applicable
    ,homepage_segment           => ''                   --> set to '' if not applicable
);


-- PLUTO GLOBAL
CALL udw_clientsolutions_cs.sp_partner_get_cdw_weekly_reports(
    partner                     => 'pluto'
    ,region                     => 'cdw_eu'
    ,report_interval            => 'weekly'
    ,start_date                 => '2024-09-23'         --> if '' default dates provided by sp
    ,end_date                   => '2024-09-29'         --> if '' default dates provided by sp
    ,countries                  => 'GB'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@udw_marketing_analytics_reports.pluto_external_us/'
    ,int_stage                  => '@udw_marketing_analytics_reports.pluto_external_international/'
    ,file_name_prefix           => 'pluto_'
    ,attribution_window_days    => 7
    ,lookback_window_months     => 12
    ,page_visit_lookback_days   => 30
    ,operative_table            => 'udw_clientsolutions_cs.pluto_operative_sales_orders'
    ,mapping_table              => 'udw_clientsolutions_cs.pluto_custom_creative_mapping'
    ,exposure_table             => 'udw_clientsolutions_cs.pluto_custom_global_exposure'
    ,app_usage_table            => 'udw_clientsolutions_cs.pluto_custom_app_usage'
    ,app_name                   => 'Pluto TV'
    ,signup_segment             => ''              --> set to '' if not applicable
    ,homepage_segment           => ''              --> set to '' if not applicable
);



