-- 23,505,843 records

with source as (
    select 
    d.name as global_dimension_2_code_name,
    d.dimension_code, 
    a.*,
    
    from {{ source('sql_erp_prod_dbo', 'petshop_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972') }} as a
    left join {{ ref('stg_petshop_dimension_value') }} as d on  a.global_dimension_2_code = d.code
),

renamed as (
    select

    case when global_dimension_2_code_name in ('POS Sale')  then location_code end as offline_order_channel,
        
        drop_shipment,
        source_no_,
        document_no_,
        sales_amount__actual_,
        source_code, -- INVTADJMT, SALES, RECLASSJNL, PURCHASES, BACKOFFICE, ITEMJNL, REVALJNL, TRANSFER
        gen__prod__posting_group, --SHIPPING, NON FOOD
        gen__bus__posting_group, -- DOMESTIC, RETAIL, DOM-SER, FOREIGN, B2B, AFFILIATES, P&M-SER, FOR-SER, DOM-CONS, PET - REL

        global_dimension_2_code_name, --from dimension_value
        global_dimension_2_code,
        dimension_code,  --from dimension_value

       -- Revenue source categorization
       case 
            when global_dimension_2_code_name in ('Amazon FBA','Souq/Amazon','Instashop','El Grocer','Careem','Now Now','Deliveroo','Talabat') then 'MKP' -- Affiliate, MKP, 3rd Party
            when global_dimension_2_code_name in ('POS Sale') then 'Shop' --Retail Store, Shop
            when global_dimension_2_code_name in ('Online') and document_type in( 2,4) then 'Online' --Email: Validation Required for Unusual Document Prefixes in Value Entry
            when global_dimension_2_code_name in ('B2B Sales') then 'B2B'
            when global_dimension_2_code_name in ('Project & Maintenance','Pet Relocation') then 'Service'
            else 'Cheack My Logic'
        end as sales_channel,  --revenue_source
        --sales_channel_details



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
        
        -- Location
        location_code,
        
        -- Item ledger entry type transformation
        case 
            when item_ledger_entry_type = 0 then 'Purchase'
            when item_ledger_entry_type = 1 then 'Sale'
            when item_ledger_entry_type = 2 then 'Positive Adjmt.'
            when item_ledger_entry_type = 3 then 'Negative Adjmt.'
            when item_ledger_entry_type = 4 then 'Transfer'
            else 'cheak my logic'
        end as item_ledger_entry_type,
        
        -- Global dimension 2 code transformation
        


        

        
        -- Document type transformation (detailed)
        case 
            when document_type = 0 and document_no_ like 'OPEN%' then 'OPENING'
            when document_type = 0 and document_no_ like 'INV%' then 'SS - Sales Shipment'
            
            when document_type = 1 then 'SS - Sales Shipment'
            
            when document_type = 2 and document_no_ like 'INV%' then 'INV - Online Posted Sales Invoice'
            when document_type = 2 and document_no_ like 'PSI%' then 'PSI - Offline Posted Sales Invoice'
            
            when document_type = 3 then 'PSR - Posted Sales Return'
            
            when document_type = 4 and document_no_ like 'RSO%' then 'RSO - Return Sales Order'
            when document_type = 4 and document_no_ like 'PSCM%' then 'PSCM - Posted Sales Credit Memo'
            
            when document_type = 5 and document_no_ like 'DIPGP%' then 'DIPGP'
            when document_type = 5 and document_no_ like 'DMLG%' then 'DMLG'
            when document_type = 5 and document_no_ like 'HQWGP%' then 'HQWGP'
            when document_type = 5 and document_no_ like 'SZRGP%' then 'SZRGP'
            when document_type = 5 and document_no_ like 'FZNGP%' then 'FZNGP'
            when document_type = 5 and document_no_ like 'SZR_G_P%' then 'SZR_G_P'
            when document_type = 5 and document_no_ like 'RAK_G_P%' then 'RAK_G_P'
            when document_type = 5 and document_no_ like 'W2GP%' then 'W2GP'
            when document_type = 5 and document_no_ like 'QGRN_P%' then 'QGRN_P'
            when document_type = 5 and document_no_ like 'REMGP%' then 'REMGP'
            when document_type = 5 and document_no_ like 'UMGP%' then 'UMGP'
            
            when document_type = 6 then 'PPI - Posted Purchase Invoice'
            when document_type = 7 then 'PPS - Posted Purchase Shipment'
            when document_type = 8 then 'PPCM - Posted Purchase Credit Memo'
            when document_type = 9 then 'TS - Transfer Shipment'
            when document_type = 10 then 'TR - Transfer Receipt'
            else 'cheak my logic'
        end as new_document_type,
        
        -- Document type transformation (simple)
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
        end as document_type_2,
        
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
        document_type,
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

--where document_no_ in ('DIP-DT03-114334','CRK-CK02-8716') 

--where source_code = 'SALES'
--where source_no_ = 'C000106034'
