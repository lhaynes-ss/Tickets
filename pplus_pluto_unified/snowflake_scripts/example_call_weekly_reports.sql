

CALL udw_prod.udw_clientsolutions_cs.sp_partner_get_weekly_reports(
    partner                     => 'paramount'
    ,region                     => 'udw_na'
    ,report_interval            => 'weekly'
    ,start_date                 => '2024-08-26'         --> if '' default dates provided by sp
    ,end_date                   => '2024-09-01'         --> if '' default dates provided by sp
    ,countries                  => 'US'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@udw_marketing_analytics_reports.paramount_plus_external_us/paramount_plus_us/'
    ,int_stage                  => '@udw_marketing_analytics_reports.paramount_plus_external_international/paramount-plus-international/'
    ,file_name_prefix           => 'paramount_plus_'
    ,attribution_window_days    => 7
    ,lookback_window_months     => 12
    ,page_visit_lookback_days   => 30
    ,operative_table            => 'udw_prod.udw_clientsolutions_cs.paramount_operative_sales_orders'
    ,mapping_table              => 'udw_prod.udw_clientsolutions_cs.paramount_custom_creative_mapping'
    ,app_name                   => 'Paramount+'
    ,signup_segment             => '52832'              --> set to '' if not applicable
    ,homepage_segment           => '52833'              --> set to '' if not applicable
);