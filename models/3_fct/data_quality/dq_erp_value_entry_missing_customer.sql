select
distinct
document_date,
document_no_,
source_no_,
--sales_amount__actual_,
location_code,
entry_type,
entry_type_2,
FROM `tps-data-386515`.`dbt_dev_datago_stg`.`stg_erp_value_entry` AS a
    LEFT JOIN `tps-data-386515`.`dbt_dev_datago_stg`.`stg_erp_inbound_sales_header` AS b 
        ON a.document_no_ = b.documentno
    LEFT JOIN `tps-data-386515`.`dbt_dev_datago_stg`.`int_erp_customer` AS c 
        ON a.source_no_ = c.no_
    WHERE 
        a.source_code NOT IN ('INVTADMT', 'INVTADJMT')
        AND a.item_ledger_entry_type = 'Sale' 
        AND sales_channel IN ('Shop','Online')
        AND source_no_ = ''
        --AND document_no_ = 'FZN-FZ02-11312'

        order by 1 desc