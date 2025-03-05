
--23,505,843 recourd

with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
    document_no_,
    item_no_,

case 

    when document_type = 1 then 'SS - Sales Shipment'
    when document_type = 2 and  document_no_ like 'INV%' then 'INV - Online Posted Sales Invoice'
    when document_type = 2 and  document_no_ like 'PSI%' then 'PSI - Offline Posted Sales Invoice'
    when document_type = 3 then 'PSR - Posted Sales Return'
    when document_type = 4 and  document_no_ like 'RSO%' then 'RSO - Return Sales Order'
    when document_type = 4 and  document_no_ like 'PSCM%' then 'PSCM - Posted Sales Credit Memo'

    when document_type = 5 and  document_no_ like 'DIPGP%' then 'DIPGP'
    when document_type = 5 and  document_no_ like 'DMLG%' then 'DIPGP'
    when document_type = 5 and  document_no_ like 'HQWGP%' then 'HQWGP'
    when document_type = 5 and  document_no_ like 'SZRGP%' then 'SZRGP'
    when document_type = 5 and  document_no_ like 'FZNGP%' then 'FZNGP'
    when document_type = 5 and  document_no_ like 'SZR_G_P%' then 'SZR_G_P'
    when document_type = 5 and  document_no_ like 'RAK_G_P%' then 'RAK_G_P'
    when document_type = 5 and  document_no_ like 'W2GP%' then 'W2GP'
    when document_type = 5 and  document_no_ like 'QGRN_P%' then 'QGRN_P'
    when document_type = 5 and  document_no_ like 'REMGP%' then 'REMGP'
    when document_type = 5 and  document_no_ like 'UMGP%' then 'UMGP'

    when document_type = 6 then 'PPI - Posted Purchase Invoice'

    when document_type = 7 then 'PPS - Posted Purchase Shipment'
    when document_type = 8 then 'PPCM - Posted Purchase Credit Memo'
    when document_type = 9 then 'TS - Transfer Shipment'
    when document_type = 10 then 'TR - Transfer Receipt'
    else 'cheak my logic'
    end as new_document_type,

document_date,
posting_date,
valuation_date,

    sales_amount__actual_,
    cost_amount__actual_,
valued_quantity,
invoiced_quantity,
purchase_amount__actual_,
discount_amount,

cost_per_unit,
    location_code,
    
inventory_posting_group,
source_posting_group,

source_code,


        entry_no_,
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        adjustment,
        applies_to_entry,
        average_cost_exception,
        capacity_ledger_entry_no_,
        
        cost_amount__actual___acy_,
        cost_amount__expected_,
        cost_amount__expected___acy_,
        cost_amount__non_invtbl__,
        cost_amount__non_invtbl___acy_,
        
        cost_per_unit__acy_,
        cost_posted_to_g_l,
        cost_posted_to_g_l__acy_,
        description,
        dimension_set_id,
        
        
        document_line_no_,
        
        document_type,
        entry_type,
        drop_shipment,
        
        exp__cost_posted_to_g_l__acy_,
        expected_cost,
        expected_cost_posted_to_g_l,
        external_document_no_,
        gen__bus__posting_group,
        gen__prod__posting_group,
        global_dimension_1_code,
        global_dimension_2_code,
        inventoriable,
        
        
        item_charge_no_,
        item_ledger_entry_no_,
        item_ledger_entry_quantity,
        item_ledger_entry_type,
        
        job_ledger_entry_no_,
        job_no_,
        job_task_no_,
        journal_batch_name,
        
        no_,
        order_line_no_,
        order_no_,
        order_type,
        partial_revaluation,
        
        
        purchase_amount__expected_,
        reason_code,
        return_reason_code,
        
        sales_amount__expected_,
        salespers__purch__code,
        
        source_no_,
        
        source_type,
        timestamp,
        type,
        user_id,
        
        valued_by_average_cost,
        
        variance_type,
        variant_code

    from source

)

select * from renamed

