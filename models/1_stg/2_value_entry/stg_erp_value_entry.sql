-- 23,505,843 records

with source as (
    select 
    d.dimension_code, 
    d.name as global_dimension_2_code_name,

            CASE
            --3rd Party
                WHEN a.source_no_ = 'BCN/2021/4059' OR d.name = 'Instashop' THEN 'Instashop'
                WHEN a.source_no_ = 'BCN/2021/4060' OR d.name = 'El Grocer' THEN 'El Grocer'
                WHEN a.source_no_ = 'BCN/2021/4408' OR d.name IN ('Noon', 'NOWNOW', 'Now Now') THEN 'Noon'
                WHEN a.source_no_ = 'BCN/2021/4063' OR d.name = 'Careem' THEN 'Careem' --
                WHEN a.source_no_ = 'BCN/2021/4064' OR d.name = 'Talabat' THEN 'Talabat' --
                WHEN a.source_no_ = 'BCN/2021/4067' OR d.name = 'Deliveroo' THEN 'Deliveroo'
                WHEN a.source_no_ = 'BCN/2021/4061' OR d.name = 'Swan Inc' THEN 'Swan'

                when a.gen__bus__posting_group = 'B2B' then 'B2B Sales'

                WHEN a.source_no_ = 'BCN/2021/4066' THEN 'Amazon DFS'
                WHEN a.source_no_ = 'BCN/2024/4064' OR d.name = 'Amazon FBA'  THEN 'Amazon FBA'
                WHEN a.source_no_ = 'BCN/2021/0691' OR d.name IN ('SOUQ', 'Souq/Amazone') THEN 'Amazon'
                            
            ELSE d.name
        END AS clc_global_dimension_2_code_name,

    a.*,
    
    from {{ source('sql_erp_prod_dbo', 'petshop_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972') }} as a
    left join {{ ref('stg_dimension_value') }} as d on  a.global_dimension_2_code = d.code and d.global_dimension_no_ = 2
),

renamed as (
    select

        case 
            when item_ledger_entry_type = 0 then 'Purchase'
            when item_ledger_entry_type = 1 then 'Sale'
            when item_ledger_entry_type = 2 then 'Positive Adjmt.'
            when item_ledger_entry_type = 3 then 'Negative Adjmt.'
            when item_ledger_entry_type = 4 then 'Transfer'
            else 'cheak my logic'
        end as item_ledger_entry_type,


        case 
            when document_type = 0 then '----'
            when document_type = 1 then 'Sales Shipment'
            when document_type = 2 then 'Sales Invoice'
            when document_type = 3 then 'Sales Return Receipt'
            when document_type = 4 then 'Sales Credit Memo'
            when document_type = 5 then 'Purchase Receipt'
            when document_type = 6 then 'Purchase Invoice'
            when document_type = 7 then 'Purchase Return Shipment'
            when document_type = 8 then 'Purchase Credit Memo'
            when document_type = 9 then 'Transfer Shipment'
            when document_type = 10 then 'Transter Receipt'
            else 'cheak my logic'
        end as document_type,


        -- Transaction type classification with improved logic for negative amounts
CASE 
    -- Handle document_type = 0 with amount check
    WHEN document_type = 0 AND sales_amount__actual_ >= 0 THEN 'Sale'
    WHEN document_type = 0 AND sales_amount__actual_ < 0 THEN 'Refund'
    
    -- Standard document type classifications
    WHEN document_type = 2 THEN 'Sale'
    WHEN document_type = 4 THEN 'Refund'
    
    -- Everything else
    ELSE 'Other'
END AS transaction_type,





CASE
        WHEN clc_global_dimension_2_code_name IN ('Amazon DFS', 'Amazon') THEN 'DIP'
        WHEN clc_global_dimension_2_code_name = 'Pet Relocation' THEN 'PRL'
        ELSE location_code
    END AS clc_location_code,

location_code,


    case when clc_global_dimension_2_code_name in ('POS Sale')  then location_code end as offline_order_channel,
        
        drop_shipment,
        source_no_,
        document_no_,
        sales_amount__actual_,
        source_code, -- INVTADJMT, SALES, RECLASSJNL, PURCHASES, BACKOFFICE, ITEMJNL, REVALJNL, TRANSFER
        gen__prod__posting_group, --SHIPPING, NON FOOD
        gen__bus__posting_group, -- DOMESTIC, RETAIL, DOM-SER, FOREIGN, B2B, AFFILIATES, P&M-SER, FOR-SER, DOM-CONS, PET - REL

        clc_global_dimension_2_code_name, --from dimension_value
        global_dimension_2_code_name, --from dimension_value
        global_dimension_2_code,
        dimension_code,  --from dimension_value




       -- Revenue source categorization
       case 
            when clc_global_dimension_2_code_name in ('Online')  then 'Online' 
            when clc_global_dimension_2_code_name in ('POS Sale') then 'Shop'
            when clc_global_dimension_2_code_name in ('Amazon', 'Amazon FBA','Amazon DFS', 'Swan' ,'Instashop','El Grocer','Careem','Noon','Deliveroo','Talabat','BlazeApp') then 'Affiliate' -- Affiliate, MKP, 3rd Party
            when clc_global_dimension_2_code_name in ('B2B Sales') then 'B2B'
            when clc_global_dimension_2_code_name in ('Project & Maintenance','Pet Relocation','Cleaning & Potty','PETRELOC','Grooming','Mobile Grooming','Shop Grooming') then 'Service'
            else 'Cheack My Logic'
        end as sales_channel,  
        
       case 
            when clc_global_dimension_2_code_name in ('Online')  then 1
            when clc_global_dimension_2_code_name in ('POS Sale') then 2
            when clc_global_dimension_2_code_name in ('Amazon', 'Amazon FBA','Amazon DFS', 'Swan' ,'Instashop','El Grocer','Careem','Noon','Deliveroo','Talabat') then 3
            when clc_global_dimension_2_code_name in ('B2B Sales') then 4
            when clc_global_dimension_2_code_name in ('Project & Maintenance','Pet Relocation','Cleaning & Potty','PETRELOC','Grooming','Mobile Grooming','Shop Grooming') then 5
            else 6
        end as sales_channel_sort, 


        -- Core identifiers
        
        
        item_no_,
        entry_no_,
        
        -- Business posting groups
        
        
        source_posting_group, -- ONLINE_CUSTOMER, DOMESTIC, OFFLINE CUSTOMERS, B2B DOMESTIC, FOREIGN, DOMSERVICE, AFFILIATES, PETRELOCATION, P&M, DOMESTIC_CONSIGNMENT, FORESERVICE, INTERCOM, INTERLUCK, INTERPETSLUX
        inventory_posting_group,
        
        
        -- Dimensions
        global_dimension_1_code,
        dimension_set_id,
        
        -- Dates
        document_date,
        posting_date,
        valuation_date,
        
        -- Financial amounts
        
        cost_amount__actual_,
        valued_quantity,
        invoiced_quantity,
        purchase_amount__actual_,
        discount_amount,
        cost_per_unit,
        
        

        
        -- Global dimension 2 code transformation

        
    
        
        -- Entry type transformation
        case 
            when entry_type = 0 then 'Direct Cost'  
            when entry_type = 1 then 'Revaluation'
            when entry_type = 2 then 'Rounding'
            else 'cheak my logic'
        end as entry_type_2,
        
        -- Source type transformation
        case 
            when source_type = 0 then '----'  
            when source_type = 1 then 'Customer'
            when source_type = 2 then 'Vendor'
            when source_type = 37 then '37'
            when source_type = 39 then '39'
            when source_type = 5741 then '5741'
            else 'cheak my logic'
        end as source_type_2,
        
        -- Order type transformation
        case 
            when order_type = 0 then '----'  
            when order_type = 2 then 'Transfer'
            else 'cheak my logic'
        end as order_type_2,
        
        -- Type transformation
        case 
            when type = 2 then '----'  
            when type = 0 then 'Work Center'
            else 'cheak my logic'
        end as type_2,
        
        -- Raw dimension fields
        --document_type,
        order_type,
        entry_type,
        source_type,
        variance_type,
        type, -- null, Work Center
        variant_code,
        
        -- Additional cost fields
        cost_amount__actual___acy_,
        cost_amount__expected_,
        cost_amount__expected___acy_,
        cost_amount__non_invtbl__,
        cost_amount__non_invtbl___acy_,
        cost_per_unit__acy_,
        cost_posted_to_g_l,
        cost_posted_to_g_l__acy_,
        exp__cost_posted_to_g_l__acy_,
        expected_cost,
        expected_cost_posted_to_g_l,
        
        -- Purchase amounts
        purchase_amount__expected_,
        
        -- Sales amounts  
        sales_amount__expected_,
        
        -- Document references
        document_line_no_,
        external_document_no_,
        order_line_no_,
        order_no_,
        
        -- Item details
        item_charge_no_,
        item_ledger_entry_no_,
        item_ledger_entry_quantity,
        
        -- Job details
        job_ledger_entry_no_,
        job_no_,
        job_task_no_,
        
        -- Journal details
        journal_batch_name,
        
        -- Miscellaneous fields
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        adjustment,
        applies_to_entry,
        average_cost_exception,
        capacity_ledger_entry_no_,
        description,
        
        inventoriable,
        no_,
        partial_revaluation,
        reason_code,
        return_reason_code,
        salespers__purch__code,
        timestamp,
        user_id,
        valued_by_average_cost
        
    from source
)

select * from renamed
 --where document_no_ in ('PSI/2021/01307', 'PSI/2023/00937') and source_code = 'SALES'

