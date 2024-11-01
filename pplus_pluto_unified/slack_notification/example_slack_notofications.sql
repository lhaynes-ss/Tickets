
/**
error handling 1
------------------
shows example of manually raising an error, handling a specific error by name, and 
handling a generic error.
**/
DECLARE
    vaughns_fancy_exception EXCEPTION (-20008, 'You have to be fancier than that!');
BEGIN

    LET fancy_score := 0;               --> if less than 1 then this triggers exception
    -- LET fancy_score := 'fish';       --> this would trigger general exception as fish is not a number 


    -- manually test for exception condition
    IF (:fancy_score >= 1) THEN 
        RETURN 'SUCCESS';
    ELSE
        RAISE vaughns_fancy_exception;
    END IF;

-- handle exception
EXCEPTION
    WHEN vaughns_fancy_exception THEN 
        RETURN '1';
    WHEN OTHER THEN 
        RETURN '2';
END;


--===========================================================
--===========================================================


/**
error handling 2
------------------
shows example of handling error by sending a SIMPLE notification to the 
slack webhook.
**/
DECLARE
    vaughns_fancy_exception EXCEPTION (-20008, 'You have to be fancier than that!');
BEGIN

    LET fancy_score := 'fish';      --> this would trigger general exception as fish is not a number 


    -- manually test for exception condition
    IF (:fancy_score >= 1) THEN 
        RETURN 'SUCCESS';
    ELSE
        RAISE vaughns_fancy_exception;
    END IF;

-- handle exception
EXCEPTION
    WHEN vaughns_fancy_exception THEN 
        RETURN '1';
    WHEN OTHER THEN 

        SELECT udw_clientsolutions_cs.udf_submit_slack_notification_simple(
            slack_webhook_url => 'https://hooks.slack.com/triggers/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'
            ,date_string => '2024-10-30'
            ,name_string => 'Snowflake Task Monitor 1'
            ,message_string => 'This is just a test from the simple version of the function!!'
        );

        RETURN 'FAILED';

END;


--===========================================================
--===========================================================


/**
error handling 3
------------------
shows example of handling error by sending a CUSTOM notification to the 
slack webhook.
**/
DECLARE
    vaughns_fancy_exception EXCEPTION (-20008, 'You have to be fancier than that!');
BEGIN

    LET fancy_score := 'fish';      --> this would trigger general exception as fish is not a number 


    -- manually test for exception condition
    IF (:fancy_score >= 1) THEN 
        RETURN 'SUCCESS';
    ELSE
        RAISE vaughns_fancy_exception;
    END IF;

-- handle exception
EXCEPTION
    WHEN vaughns_fancy_exception THEN 
        RETURN '1';
    WHEN OTHER THEN 

        SELECT udw_clientsolutions_cs.udf_submit_slack_notification_custom(
            slack_webhook_url => 'https://hooks.slack.com/triggers/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'
            ,header_json_string => '{"Content-type": "application/json"}'
            ,data_json_string => '{
                "date": "2024-10-30",
                "message": "This is a more advanced custom test!!",
                "process_name": "Snowflake Task Monitor 2"
            }'
        );

        RETURN 'FAILED';
        
END;



