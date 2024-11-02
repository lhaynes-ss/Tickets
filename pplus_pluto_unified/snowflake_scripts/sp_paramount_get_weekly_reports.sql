/**
 Execute Paramount+ reports

**/

-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


CREATE OR REPLACE PROCEDURE udw_clientsolutions_cs.sp_paramount_get_weekly_reports()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE 
    -- specify variables used in this stored procedure
    start_date                  VARCHAR;    --> 'YYYY-MM-DD' or if '' then dates will be auto-set by the stored proc
    end_date                    VARCHAR;    --> 'YYYY-MM-DD' or if '' then dates will be auto-set by the stored proc
    partner                     VARCHAR;    --> paramount | pluto
    interval                    VARCHAR;    --> weekly | monthly
    regions                     OBJECT;     --> countries by connection. database_region where database in (udw, cdw) and region in (na, eu, nordics, apac, sa)
    max_rows                    INT;        --> max # of rows per report. Point at which report is split into additional file. 1 million - 1 for header
    attribution_window          INT;        --> max days after exposure for attribution credit
    us_stage                    VARCHAR;    --> stage for US reports s3 bucket
    int_stage                   VARCHAR;    --> stage for international reports s3 bucket
    file_name_prefix            VARCHAR;    --> string prepended to file name. Usually advertiser name
    attribution_window_days     INT;        --> number of days for conversion attribution
    lookback_window_months      INT;        --> number of months for lookback window
    page_visit_lookback_days    INT;        --> number of days for web pixel lookback
    operative_table             VARCHAR;    --> schema.table for advertiser custom operative one data
    mapping_table               VARCHAR;    --> schema.table for advertiser custom mapping data
    exposure_table              VARCHAR;    --> exposure data table migrated from CDW
    app_usage_table             VARCHAR;    --> app usage data table migrated from CDW
    app_name                    VARCHAR;    --> app name
    signup_segment              VARCHAR;    --> segment id for signup pixel or ''
    homepage_segment            VARCHAR;    --> segment id for homepage pixel or ''
    current_date                TIMESTAMP;  --> today
    region_keys                 ARRAY;      --> holds region keys for looping (e.g., ['udw_na', 'cdw_eu', ...])
    region_key                  VARCHAR;    --> holds a specific region, used for looping (e.g., 'udw_na')
    region_countries            VARCHAR;    --> holds the corresponding value for the region_key (e.g., 'US, CA')
    sp                          VARCHAR;    --> name of the stored procedure to use
    get_reports_query           VARCHAR;    --> the dynamic query string used to generate the report
    cdw_tables                  VARCHAR;    --> hold extra arguments required for CDW version of get_reports_query
    task_name                   VARCHAR;    --> the name of the task that will trigger this stored procedure
    log                         ARRAY;      --> array to store log messages for debugging
    log_message                 VARCHAR;    --> message to add to the log array. For debugging.

BEGIN

    -- get current date
    current_date := CURRENT_DATE();

    -- ==================================================================================
    -- start config 
    -- ==================================================================================
    start_date                  := '';
    end_date                    := '';
        
    partner                     := 'paramount';
    interval                    := 'weekly';
    regions                     := OBJECT_CONSTRUCT(
                                    'udw_na'        , 'US, CA'
                                    ,'cdw_eu'       , 'AT, DE, FR, GB, IT'
                                    -- ,'cdw_nordics'  , ''
                                    ,'cdw_apac'     , 'AU'
                                    ,'cdw_sa'       , 'BR'
                                );
    max_rows                    := 999999;
    attribution_window          := 7;

    us_stage                    := '@udw_marketing_analytics_reports.paramount_plus_external_us/';
    int_stage                   := '@udw_marketing_analytics_reports.paramount_plus_external_international/';
    file_name_prefix            := 'paramount_plus_';

    attribution_window_days     := 7;
    lookback_window_months      := 12;
    page_visit_lookback_days    := 30;
    operative_table             := 'udw_clientsolutions_cs.paramount_operative_sales_orders';
    mapping_table               := 'udw_clientsolutions_cs.paramount_custom_creative_mapping';

    exposure_table              := 'udw_clientsolutions_cs.paramount_custom_global_exposure';
    app_usage_table             := 'udw_clientsolutions_cs.paramount_custom_app_usage';

    app_name                    := 'Paramount+';
    signup_segment              := '52832';
    homepage_segment            := '52833';

    task_name                   := 'tsk_paramount_get_weekly_reports';

    -- ==================================================================================
    -- end config 
    -- ==================================================================================

    -- init logging
    log := ARRAY_CONSTRUCT();
    log_message := '';

    -- get an array of all of the keys from the regions object
    region_keys := OBJECT_KEYS(:regions);
    
    -- loop through the keys array... (e.g., ['udw_na', 'cdw_eu', ...])
    FOR num IN 0 TO ARRAY_SIZE(:region_keys) - 1 DO

        -- log message (e.g., Region udw_na started.)
        log_message := 'Region ' || :region_key || ' started.';
        log := (SELECT ARRAY_APPEND(:log, :log_message));
        
        -- get the region key (e.g., 'udw_na')
        region_key := GET(:region_keys, num);

        -- get the countries list (e.g., 'US, CA')
        region_countries := GET(:regions, :region_key);

        -- specify stored procedure to use depending on region
        sp := 'sp_partner_get_weekly_reports';

        -- specify cdw tables to use depending on region
        cdw_tables := '';
        
        IF(:region_key <> 'udw_na') THEN 
            sp := 'sp_partner_get_cdw_weekly_reports';

            cdw_tables := '
                ,exposure_table             => ''' || :exposure_table || '''
                ,app_usage_table            => ''' || :app_usage_table || '''
            ';
            
        END IF;

        -- query
        get_reports_query := '
            CALL udw_clientsolutions_cs.' || :sp || '(
                partner                     => ''' || :partner || '''
                ,region                     => ''' || :region_key || '''
                ,report_interval            => ''' || :interval || '''
                ,start_date                 => ''' || :start_date || '''
                ,end_date                   => ''' || :end_date || '''
                ,countries                  => ''' || :region_countries || '''
                ,max_rows                   => '   || :max_rows || '
                ,attribution_window         => '   || :attribution_window || '
                ,us_stage                   => ''' || :us_stage || '''
                ,int_stage                  => ''' || :int_stage || '''
                ,file_name_prefix           => ''' || :file_name_prefix || '''
                ,attribution_window_days    => '   || :attribution_window_days || '  
                ,lookback_window_months     => '   || :lookback_window_months || '   
                ,page_visit_lookback_days   => '   || :page_visit_lookback_days || ' 
                ,operative_table            => ''' || :operative_table || '''        
                ,mapping_table              => ''' || :mapping_table || ''' 
                ' || :cdw_tables || '       
                ,app_name                   => ''' || :app_name || '''               
                ,signup_segment             => ''' || :signup_segment || '''         
                ,homepage_segment           => ''' || :homepage_segment || '''       
            );
        ';


        -- RETURN :get_reports_query;              --> uncomment this line for testing

        EXECUTE IMMEDIATE :get_reports_query;   --> production

        -- log message (e.g., Region udw_na completed.)
        log_message := 'Region ' || :region_key || ' completed.';
        log := (SELECT ARRAY_APPEND(:log, :log_message));


    END FOR;

    RETURN 'SUCCESS';


-- handle exception
EXCEPTION
    WHEN OTHER THEN
        SELECT udw_clientsolutions_cs.udf_submit_slack_notification_simple(
            slack_webhook_url => 'https://hooks.slack.com/triggers/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'
            ,date_string => :current_date::VARCHAR
            ,name_string => 'Snowflake Task Monitor'
            ,message_string => 'Task "' || :task_name || '" failed.' || 
                ' Error: (' || :SQLCODE || ', ' || :SQLERRM || ')' ||
                ' || LOG: (' || ARRAY_TO_STRING(:log, ' => ') || ')'
        );

        RETURN 'FAILED WITH ERROR(' || :SQLCODE || ', ' || :SQLERRM || ')';

END;
$$;
