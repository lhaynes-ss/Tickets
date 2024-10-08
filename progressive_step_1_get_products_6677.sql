USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;


/***** can only pull creative info from Jan'23 and onwards as of 07/25/2023 due to the data in xdeivce table! *****/
-- The entire query takes ~10min to run on Medium size warehouse if you need ALL info.
/******************************************************************************************
* Choose either VAO Number or Samsung Order Number, and then change the comment in line 11 & 88.
    [vao = $reporting_vao] OR[samsung_campaign_id = $reporting_samsung_campaign_id]
******************************************************************************************/
SET reporting_vao = 87932;  -- [SET reporting_samsung_campaign_id = 'SCID0021529';]

/*******************************
### CTE part starts from line 73, and main query part starts from line 196.
### All CTE parts (except for "creative") are using sales_order_id as the key to join.
### Remember to comment out the parts you DON'T need in BOTH CTE and main query parts for your report to reduce the query running time!

### Here are the column names in each CTE:
- vao_samsungCampaignID:     *** Don't comment out this part in the TEMP table below
    vao,                     -- ex: 90291
    samsung_campaign_id,     -- ex: 'SCID0021529'
    sales_order_id,
    sales_order_name

- salesOrder:
    sales_order_id,
    sales_order_name,
    order_start_date,
    order_end_date,
    time_zone

- cmpgn:                    *** Don't comment out this part in the TEMP table below if you need "Flight" and/or "Creative" and/or "Line Item" info!
    sales_order_id,
    campaign_id,
    campaign_name,
    rate_type,
    net_unit_cost,
    cmpgn_start_datetime_utc,
    cmpgn_end_datetime_utc

- flight:
    sales_order_id,
    flight_id,
    flight_name,
    flight_start_datetime_utc,
    flight_end_datetime_utc

- cmpgn_flight_creative:     *** Don't comment out this part in the TEMP table below if you need "Creative" info!
    cmpgn.sales_order_id,
    campaign_id,
    flight_id,
    creative_id

- creative:
    sales_order_id,
    creative_id,
    creative_name

- lineItem:
    sales_order_id,
    sales_order_line_item_id,
    sales_order_line_item_name,
    sales_order_line_item_start_datetime_utc,
    sales_order_line_item_end_datetime_utc
******************************/

/******************************************************************************************
* Create campaign_meta TEMP table.
******************************************************************************************/
DROP TABLE IF EXISTS campaign_meta;
CREATE TEMP TABLE campaign_meta AS (

    WITH vao_samsungCampaignID AS (
    SELECT
        vao,
        samsung_campaign_id,
        sales_order_id,
        sales_order_name
    FROM
        (
        SELECT
            CAST(replace(sf_opp.jira_id__c, 'VAO-', '') AS INT) AS vao,
            sf_opp.samsung_campaign_id__c AS samsung_campaign_id,
            sf_opp.operative_order_id__c AS sales_order_id,
            sf_opp.order_name__c AS sales_order_name,
            ROW_NUMBER() OVER(PARTITION BY vao ORDER BY sf_opp.lastmodifieddate DESC) AS rn
        FROM SALESFORCE.OPPORTUNITY AS sf_opp
        WHERE vao = $reporting_vao -- samsung_campaign_id = $reporting_samsung_campaign_id
        )
    WHERE rn = 1
    ),

    salesOrder AS (
    SELECT
        sales_order_id,
        sales_order_name,
        order_start_date,
        order_end_date,
        time_zone
    FROM
        (
        SELECT
            sales_order.sales_order_id,
            sales_order.sales_order_name,
            sales_order.order_start_date,
            sales_order.order_end_date,
            sales_order.time_zone,
            ROW_NUMBER() OVER(PARTITION BY sales_order.sales_order_id ORDER BY sales_order.last_modified_on DESC) AS rn
        FROM OPERATIVEONE.SALES_ORDER AS sales_order
        JOIN vao_samsungCampaignID AS vao
            USING (sales_order_id)
        ) AS foo
    WHERE foo.rn = 1
    ),

    cmpgn AS (
    SELECT DISTINCT
        sales_order_id,
        sales_order_line_item_id,
        cmpgn.id AS campaign_id,
        cmpgn.name AS campaign_name,
        rate_type,
        net_unit_cost,
        cmpgn.start_at_datetime::TIMESTAMP AS cmpgn_start_datetime_utc,
        cmpgn.end_at_datetime::TIMESTAMP AS cmpgn_end_datetime_utc
    FROM TRADER.CAMPAIGNS_LATEST AS cmpgn
    JOIN
        (
        SELECT DISTINCT
            cmpgn_att.campaign_id,
            cmpgn_att.rate_type,
            cmpgn_att.net_unit_cost,
            cmpgn_att.io_external_id AS sales_order_id,
            cmpgn_att.li_external_id AS sales_order_line_item_id
        FROM TRADER.CAMPAIGN_OMS_ATTRS_LATEST AS cmpgn_att
        JOIN vao_samsungCampaignID
            ON vao_samsungCampaignID.sales_order_id = cmpgn_att.external_id
        ) AS foo
        ON cmpgn.id = foo.campaign_id
    ),

    flight AS (
    SELECT DISTINCT
        cmpgn.sales_order_id,
        flight.id AS flight_id,
        flight.name AS flight_name,
        flight.start_at_datetime::TIMESTAMP AS flight_start_datetime_utc,
        flight.end_at_datetime::TIMESTAMP AS flight_end_datetime_utc
    FROM TRADER.FLIGHTS_LATEST AS flight
    JOIN cmpgn
        USING (campaign_id)
    ),

    cmpgn_flight_creative AS
    (
    SELECT DISTINCT
        cmpgn.sales_order_id,
        campaign_id,
        flight_id,
        creative_id
    FROM DATA_AD_XDEVICE.FACT_DELIVERY_EVENT_WITHOUT_PII AS fact
    JOIN cmpgn
        USING (campaign_id)
    WHERE fact.udw_partition_datetime BETWEEN (SELECT MIN(cmpgn_start_datetime_utc) FROM cmpgn) AND (SELECT MAX(cmpgn_end_datetime_utc) FROM cmpgn)
    ),

    creative AS (
    SELECT DISTINCT 
        cmpgn_flight_creative.sales_order_id,
        creative.id AS creative_id,
        creative.name AS creative_name
    FROM TRADER.CREATIVES_LATEST AS creative
    JOIN cmpgn_flight_creative
        ON cmpgn_flight_creative.creative_id = creative.id
    ),

    lineItem AS (
    SELECT
        sales_order_id,
        sales_order_line_item_id,
        sales_order_line_item_name,
        sales_order_line_item_start_datetime_utc,
        sales_order_line_item_end_datetime_utc,
        product_id
    FROM
        (
        SELECT
            lineItem.sales_order_id,
            lineItem.sales_order_line_item_id,
            lineItem.sales_order_line_item_name,
            lineItem.product_id,
            TIMESTAMP_NTZ_FROM_PARTS(lineItem.sales_order_line_item_start_date::date, lineItem.start_time::time) AS sales_order_line_item_start_datetime_utc,
            TIMESTAMP_NTZ_FROM_PARTS(lineItem.sales_order_line_item_end_date::date, lineItem.end_time::time) AS sales_order_line_item_end_datetime_utc,
            ROW_NUMBER() OVER(PARTITION BY lineItem.sales_order_line_item_id ORDER BY lineItem.last_modified_on DESC) AS rn
        FROM OPERATIVEONE.SALES_ORDER_LINE_ITEMS AS lineItem
        JOIN vao_samsungCampaignID AS vao
            USING (sales_order_id)
        ) AS foo
    WHERE foo.rn = 1
    ),

    products_cte AS (
        SELECT DISTINCT 
            p.product_id
            ,p.product_name 
        FROM operativeone.products p
        WHERE p.product_id IN (
            SELECT DISTINCT li.product_id 
            FROM lineItem li
        )
    )

    /******************************************************************************************
    * Main query          *** Remember to edit the parts you want to keep in below as well!
    ******************************************************************************************/
    SELECT DISTINCT
    /******************************
    * VAO info
    ******************************/
        vao_samsungCampaignID.vao,
        vao_samsungCampaignID.samsung_campaign_id,
        vao_samsungCampaignID.sales_order_id,
        vao_samsungCampaignID.sales_order_name,
    /******************************
    * Sales Order info
    ******************************/
        salesOrder.order_start_date,
        salesOrder.order_end_date,
    /******************************
    * Campaign info
    ******************************/
        cmpgn.campaign_id,
        cmpgn.campaign_name,
        cmpgn.rate_type,
        cmpgn.net_unit_cost,
        cmpgn.cmpgn_start_datetime_utc,
        cmpgn.cmpgn_end_datetime_utc,
    /******************************
    * Flight info
    ******************************/
        flight.flight_id,
        flight.flight_name,
        flight.flight_start_datetime_utc,
        flight.flight_end_datetime_utc,
    /******************************
    * Creative info
    ******************************/
        creative.creative_id,
        creative.creative_name,
    /******************************
    * Line Item info
    ******************************/
        lineItem.sales_order_line_item_id,
        lineItem.sales_order_line_item_name,
        lineItem.sales_order_line_item_start_datetime_utc,
        lineItem.sales_order_line_item_end_datetime_utc,
        lineItem.product_id,
        p.product_name
    FROM vao_samsungCampaignID
    JOIN salesOrder USING (sales_order_id)
    JOIN cmpgn USING (sales_order_id)
    JOIN flight USING (sales_order_id)
    JOIN cmpgn_flight_creative USING (sales_order_id)
    JOIN creative USING (sales_order_id)
    JOIN lineItem USING (sales_order_id, sales_order_line_item_id)
    JOIN products_cte p ON p.product_id = lineItem.product_id

);

select * from campaign_meta;