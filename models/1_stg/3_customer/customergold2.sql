SELECT 
    a.source_no_,



    -- Updated Total Order Count Logic
    (COUNT(DISTINCT CASE WHEN a.document_no_ LIKE 'INV%' THEN b.web_order_id END) +
     COUNT(DISTINCT CASE WHEN a.document_no_ NOT LIKE 'INV%' THEN a.document_no_ END)) AS total_order_count,

    -- Online Orders - Count distinct web_order_id instead of document_no_
    COUNT(DISTINCT CASE WHEN a.document_no_ LIKE 'INV%' THEN b.web_order_id END) AS online_order_count,
    
    -- Express Orders Count (Online orders with order_type = 'EXPRESS')
    COUNT(DISTINCT CASE WHEN a.document_no_ LIKE 'INV%' AND b.order_type = 'EXPRESS' THEN b.web_order_id END) AS express_order_count,
    
    -- Standard Delivery Orders Count (Online orders with order_type != 'EXPRESS')
    COUNT(DISTINCT CASE WHEN a.document_no_ LIKE 'INV%' AND (b.order_type != 'EXPRESS' OR b.order_type IS NULL) THEN b.web_order_id END) AS standard_delivery_order_count,
    
    -- Offline Orders - Keep original logic
    COUNT(DISTINCT CASE WHEN a.document_no_ NOT LIKE 'INV%' THEN a.document_no_ END) AS offline_order_count,



    -- Online Order Dates
    MIN(CASE WHEN a.document_no_ LIKE 'INV%' THEN a.posting_date END) AS first_online_order_date,
    MAX(CASE WHEN a.document_no_ LIKE 'INV%' THEN a.posting_date END) AS last_online_order_date,

    -- Express Order Dates
    MIN(CASE WHEN a.document_no_ LIKE 'INV%' AND b.order_type = 'EXPRESS' THEN a.posting_date END) AS first_express_order_date,
    MAX(CASE WHEN a.document_no_ LIKE 'INV%' AND b.order_type = 'EXPRESS' THEN a.posting_date END) AS last_express_order_date,

    -- Offline Order Dates
    MIN(CASE WHEN a.document_no_ NOT LIKE 'INV%' THEN a.posting_date END) AS first_offline_order_date,
    MAX(CASE WHEN a.document_no_ NOT LIKE 'INV%' THEN a.posting_date END) AS last_offline_order_date,

    -- Customer Acquisition Date (Earliest Transaction)
    MIN(a.posting_date) AS customer_acquisition_date,

    -- Customer Acquisition Method (Online or Offline)
    CASE 
        WHEN MIN(CASE WHEN a.document_no_ LIKE 'INV%' THEN a.posting_date END) = MIN(a.posting_date) THEN 'online'
        WHEN MIN(CASE WHEN a.document_no_ NOT LIKE 'INV%' THEN a.posting_date END) = MIN(a.posting_date) THEN 'offline'
        ELSE 'unknown'
    END AS customer_acquisition_method,

    -- Total Sales Amount Metrics
    SUM(a.sales_amount__actual_) AS total_sales_value,
    SUM(CASE WHEN a.document_no_ LIKE 'INV%' THEN a.sales_amount__actual_ ELSE 0 END) AS online_sales_value,
    SUM(CASE WHEN a.document_no_ LIKE 'INV%' AND b.order_type = 'EXPRESS' THEN a.sales_amount__actual_ ELSE 0 END) AS express_sales_value,
    SUM(CASE WHEN a.document_no_ LIKE 'INV%' AND (b.order_type != 'EXPRESS' OR b.order_type IS NULL) THEN a.sales_amount__actual_ ELSE 0 END) AS standard_delivery_sales_value,
    SUM(CASE WHEN a.document_no_ NOT LIKE 'INV%' THEN a.sales_amount__actual_ ELSE 0 END) AS offline_sales_value

FROM  `tps-data-386515`.`dbt_dev_datago_stg`.`stg_erp_value_entry` AS a
LEFT JOIN `tps-data-386515`.`dbt_dev_datago_stg`.`stg_erp_inbound_sales_header` AS b 
    ON a.document_no_ = b.documentno

where  a.source_code NOT IN ('INVTADMT') and a.item_ledger_entry_type = 'Sale' and revenue_source  IN ('Shop','Online')
--and source_no_ ='BCN/2021/4059'

GROUP BY a.source_no_
  

