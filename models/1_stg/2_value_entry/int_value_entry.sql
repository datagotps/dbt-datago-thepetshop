select

ve.source_code,

--dse1.name as store,
dse2.name as global_dimension_2_code_name, --profitcenter
dse2.code as global_dimension_2_code,

dse3.name as product_group,
e.item_no_,
e.item_name,
e.item_category,
e.item_subcategory,
e.item_brand,
e.division,



dse4.name as resource,
dse5.name as vehicle,
dse6.name as costccenter,
dse7.name as project,


    CASE
        --3rd Party
        WHEN ve.source_no_ = 'BCN/2021/4059' OR dse2.name = 'Instashop' THEN 'Instashop'
        WHEN ve.source_no_ = 'BCN/2021/4060' OR dse2.name = 'El Grocer' THEN 'El Grocer'
        WHEN ve.source_no_ = 'BCN/2021/4408' OR dse2.name IN ('Noon', 'NOWNOW', 'Now Now') THEN 'Noon'
        WHEN ve.source_no_ = 'BCN/2021/4063' OR dse2.name = 'Careem' THEN 'Careem' --
        WHEN ve.source_no_ = 'BCN/2021/4064' OR dse2.name = 'Talabat' THEN 'Talabat' --
        WHEN ve.source_no_ = 'BCN/2021/4067' OR dse2.name = 'Deliveroo' THEN 'Deliveroo'
        WHEN ve.source_no_ = 'BCN/2021/4061' OR dse2.name = 'Swan Inc' THEN 'Swan'

        WHEN ve.source_no_ = 'BCN/2021/4066' THEN 'Amazon DFS'
        WHEN ve.source_no_ = 'BCN/2024/4064' OR dse2.name = 'Amazon FBA'  THEN 'Amazon FBA'
        WHEN ve.source_no_ = 'BCN/2021/0691' OR dse2.name IN ('SOUQ', 'Souq/Amazone') THEN 'Amazon'
                                    
        ELSE dse2.name
    END AS clc_global_dimension_2_code_name,

       -- Revenue source categorization
       case 
            when dse2.name in ('Online')  then 'Online' 
            when dse2.name in ('POS Sale') then 'Shop'
            when dse2.name in ('Amazon', 'Amazon FBA','Amazon DFS', 'Swan' ,'Instashop','El Grocer','Careem','Noon','Deliveroo','Talabat') then 'Affiliate' -- Affiliate, MKP, 3rd Party
            when dse2.name in ('B2B Sales') then 'B2B'
            when dse2.name in ('Project & Maintenance','Pet Relocation','Cleaning & Potty','PETRELOC','Grooming','Mobile Grooming','Shop Grooming') then 'Service'
            else 'Cheack My Logic'
        end as sales_channel,  
        
       case 
            when dse2.name in ('Online')  then 1
            when dse2.name in ('POS Sale') then 2
            when dse2.name in ('Amazon', 'Amazon FBA','Amazon DFS', 'Swan' ,'Instashop','El Grocer','Careem','Noon','Deliveroo','Talabat') then 3
            when dse2.name in ('B2B Sales') then 4
            when dse2.name in ('Project & Maintenance','Pet Relocation','Cleaning & Potty','PETRELOC','Grooming','Mobile Grooming','Shop Grooming') then 5
            else 6
        end as sales_channel_sort, 


case 
            when ve.item_ledger_entry_type = 0 then 'Purchase'
            when ve.item_ledger_entry_type = 1 then 'Sale'
            when ve.item_ledger_entry_type = 2 then 'Positive Adjmt.'
            when ve.item_ledger_entry_type = 3 then 'Negative Adjmt.'
            when ve.item_ledger_entry_type = 4 then 'Transfer'
            else 'cheak my logic'
        end as item_ledger_entry_type,


        case 
            when ve.document_type = 0 then '----'
            when ve.document_type = 1 then 'Sales Shipment'
            when ve.document_type = 2 then 'Sales Invoice'
            when ve.document_type = 3 then 'Sales Return Receipt'
            when ve.document_type = 4 then 'Sales Credit Memo'
            when ve.document_type = 5 then 'Purchase Receipt'
            when ve.document_type = 6 then 'Purchase Invoice'
            when ve.document_type = 7 then 'Purchase Return Shipment'
            when ve.document_type = 8 then 'Purchase Credit Memo'
            when ve.document_type = 9 then 'Transfer Shipment'
            when ve.document_type = 10 then 'Transter Receipt'
            else 'cheak my logic'
        end as document_type,

        -- Transaction type classification
        CASE 
            --WHEN dse2.name  = 'POS Sale' THEN 'Sale'
            WHEN ve.document_type = 0 THEN 'Sale'
            WHEN ve.document_type = 2 THEN 'Sale'
            WHEN ve.document_type = 4 THEN 'Refund'
            ELSE 'Other'
        END AS transaction_type,



CASE
        WHEN dse2.name IN ('Amazon DFS', 'Amazon') THEN 'DIP'
        WHEN dse2.name = 'Pet Relocation' THEN 'PRL'
        ELSE location_code
    END AS clc_location_code,

    location_code,


    case when dse2.name in ('POS Sale')  then location_code end as offline_order_channel,
        




        
        
        


FROM {{ ref('stg_value_entry') }} as ve
--LEFT JOIN  {{ ref('int_dimension_set_entry') }}  as dse1 on ve.dimension_set_id = dse1.dimension_set_id and dse1.global_dimension_no_ = 1 -- <STORE>
LEFT JOIN {{ ref('int_dimension_set_entry') }} as dse2 on ve.dimension_set_id = dse2.dimension_set_id and dse2.global_dimension_no_ = 2 -- <PROFITCENTER>
LEFT JOIN {{ ref('int_dimension_set_entry') }} as dse3 on ve.dimension_set_id = dse3.dimension_set_id and dse3.global_dimension_no_ = 3 -- <PRODUCTGROUP>
LEFT JOIN {{ ref('int_dimension_set_entry') }} as dse4 on ve.dimension_set_id = dse4.dimension_set_id and dse4.global_dimension_no_ = 4 -- <RESOURCE>
LEFT JOIN {{ ref('int_dimension_set_entry') }} as dse5 on ve.dimension_set_id = dse5.dimension_set_id and dse5.global_dimension_no_ = 5 -- <VEHICLE>
LEFT JOIN {{ ref('int_dimension_set_entry') }} as dse6 on ve.dimension_set_id = dse6.dimension_set_id and dse6.global_dimension_no_ = 6 -- <COSTCENTER>
LEFT JOIN {{ ref('int_dimension_set_entry') }} as dse7 on ve.dimension_set_id = dse7.dimension_set_id and dse7.global_dimension_no_ = 7 -- <PROJECT>

LEFT JOIN  {{ ref('stg_erp_inbound_sales_header') }}  as b on ve.document_no_ = b.documentno
LEFT JOIN {{ ref('int_erp_customer') }} AS c ON ve.source_no_ = c.no_
LEFT JOIN  {{ ref('int_items') }} as e on  e.item_no_ = ve.item_no_



--where dse2.dimension_code is not null

--global_dimension_no_ = 1 > dimension_code <STORE>
--global_dimension_no_ = 2 > dimension_code <PROFITCENTER>
--global_dimension_no_ = 3 > dimension_code <PRODUCT GROUP>
--global_dimension_no_ = 4 > dimension_code <RESOURCE>
--global_dimension_no_ = 5 > dimension_code <VEHICLE>
--global_dimension_no_ = 6 > dimension_code <COSTCENTER>
--global_dimension_no_ = 7 > dimension_code <PROJECT>


