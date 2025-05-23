-- 23,505,843 records

with source as (
    select * 
    from {{ source('sql_erp_prod_dbo', 'petshop_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972') }}
),

renamed as (
    select
        sales_amount__actual_,
        source_code, -- INVTADJMT, SALES, RECLASSJNL, PURCHASES, BACKOFFICE, ITEMJNL, REVALJNL, TRANSFER
        gen__prod__posting_group, --SHIPPING, NON FOOD


        -- Core identifiers
        source_no_,
        document_no_,
        item_no_,
        entry_no_,
        
        -- Business posting groups
        gen__bus__posting_group, -- DOMESTIC, RETAIL, DOM-SER, FOREIGN, B2B, AFFILIATES, P&M-SER, FOR-SER, DOM-CONS, PET - REL
        
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
        case 
            when global_dimension_2_code = '122000' then 'POS Sale'
            when global_dimension_2_code = '110000' then 'Online'
            when global_dimension_2_code = '120010' then 'B2B Sales'
            
            -- Third Party
            when global_dimension_2_code = '112125' then 'Amazon FBA'
            when global_dimension_2_code = '112120' then 'Souq/Amazon'
            when global_dimension_2_code = '112130' then 'Instashop'
            when global_dimension_2_code = '112140' then 'El Grocer'
            when global_dimension_2_code = '112150' then 'Careem'
            when global_dimension_2_code = '112170' then 'Now Now' -- NooN
            when global_dimension_2_code = '112185' then 'Deliveroo'
            when global_dimension_2_code = '112180' then 'Talabat' 
            
            -- Service    
            when global_dimension_2_code = '123030' then 'Project & Maintenance'
            when global_dimension_2_code = '123040' then 'Pet Relocation'
            else global_dimension_2_code 
        end as global_dimension_2_code,
        
        -- Revenue source categorization
        case 
            when global_dimension_2_code in ('112125','112120','112130','112140','112150','112170','112185','112180') then 'Affiliate'
            when global_dimension_2_code in ('122000') then 'Shop'
            when global_dimension_2_code in ('110000') then 'Online'
            when global_dimension_2_code in ('120010') then 'B2B'
            when global_dimension_2_code in ('123030','123040') then 'Services'
            else 'Cheack My Logic'
        end as revenue_source,
        
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
        drop_shipment,
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


--where source_code = 'SALES'