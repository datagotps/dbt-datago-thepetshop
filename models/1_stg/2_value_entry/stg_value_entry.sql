with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
        -- Core Identifiers
        entry_no_,
        document_no_,
        document_line_no_,
        order_no_,
        order_line_no_,
        external_document_no_,

        -- Item Information
        item_no_,
        item_charge_no_,
        item_ledger_entry_no_,
        item_ledger_entry_quantity,
        variant_code,
        description,

        -- Transaction Types & Status
        document_type,
        order_type,
        entry_type,
        source_type,
        item_ledger_entry_type, --Purchase, Sale, Positive Adjmt., Negative Adjmt., Transfer
        variance_type,
        type,
        source_code, --SALES, BACKOFFICE, PURCHASES, TRANSFER, INVTADJMT, RECLASSJNL, ITEMJNL, REVALJNL
        reason_code,
        return_reason_code,

        -- Dates
        document_date,
        posting_date,
        valuation_date,

        -- Quantities
        valued_quantity,
        invoiced_quantity,

        -- Financial Amounts (Local Currency)
        sales_amount__actual_,
        sales_amount__expected_,
        purchase_amount__actual_,
        purchase_amount__expected_,
        cost_amount__actual_,
        cost_amount__expected_,
        cost_amount__non_invtbl__,
        discount_amount,
        cost_per_unit,

        -- Financial Amounts (Additional Currency)
        cost_amount__actual___acy_,
        cost_amount__expected___acy_,
        cost_amount__non_invtbl___acy_,
        cost_per_unit__acy_,

        -- General Ledger Posting
        cost_posted_to_g_l,
        cost_posted_to_g_l__acy_,
        exp__cost_posted_to_g_l__acy_,
        expected_cost_posted_to_g_l,
        expected_cost,

        -- Posting Groups
        gen__prod__posting_group,
        gen__bus__posting_group,
        source_posting_group,
        inventory_posting_group,

        -- Dimensions
        global_dimension_1_code,
        global_dimension_2_code,
        dimension_set_id,

        -- Location & Logistics
        location_code,
        drop_shipment,

        -- Customer/Vendor Information
        source_no_,
        salespers__purch__code,

        -- Job/Project Related
        job_no_,
        job_task_no_,
        job_ledger_entry_no_,

        -- Journal Information
        journal_batch_name,

        -- Inventory Flags & Settings
        inventoriable,
        adjustment,
        applies_to_entry,
        average_cost_exception,
        partial_revaluation,
        valued_by_average_cost,

        -- Other References
        no_,
        capacity_ledger_entry_no_,

        -- System & Audit Fields
        user_id,
        timestamp,
        _systemid,
        _fivetran_deleted,
        _fivetran_synced,
    
    
    
    from source

)

select * from renamed
