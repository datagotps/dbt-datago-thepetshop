-- Find OFS customers that are NOT in ERP
WITH ofs_customers AS (
    SELECT DISTINCT
        customerid,
        emailid
    FROM `tps-data-386515`.`dbt_dev_datago_stg`.`int_ofs_customer`
),
erp_web_customers AS (
    SELECT DISTINCT
        web_customer_no_
    FROM `tps-data-386515`.`dbt_dev_datago_stg`.`int_erp_customer`
    WHERE web_customer_no_ IS NOT NULL 
      AND web_customer_no_ != ''
)
SELECT 
    ofs.customerid,
    ofs.emailid
FROM ofs_customers ofs
LEFT JOIN erp_web_customers erp
    ON ofs.customerid = erp.web_customer_no_
WHERE erp.web_customer_no_ IS NULL