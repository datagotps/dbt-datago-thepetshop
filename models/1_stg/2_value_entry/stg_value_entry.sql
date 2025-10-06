with 

-- Petshop source
source_petshop as (
    select 
       'Petshop' AS company_source,
        d.dimension_code, 
        d.name as global_dimension_2_code_name,
        CAST(CASE
            --3rd Party
            WHEN a.source_no_ = 'BCN/2021/4059' OR d.name = 'Instashop' THEN 'Instashop'
            WHEN a.source_no_ = 'BCN/2021/4060' OR d.name = 'El Grocer' THEN 'El Grocer'
            WHEN a.source_no_ = 'BCN/2021/4408' OR d.name IN ('Noon', 'NOWNOW', 'Now Now') THEN 'Noon'
            WHEN a.source_no_ = 'BCN/2021/4063' OR d.name = 'Careem' THEN 'Careem'
            WHEN a.source_no_ = 'BCN/2021/4064' OR d.name = 'Talabat' THEN 'Talabat'
            WHEN a.source_no_ = 'BCN/2021/4067' OR d.name = 'Deliveroo' THEN 'Deliveroo'
            WHEN a.source_no_ = 'BCN/2021/4061' OR d.name = 'Swan Inc' THEN 'Swan'
            WHEN a.gen__bus__posting_group = 'B2B' THEN 'B2B Sales'
            WHEN a.source_no_ = 'BCN/2021/4066' THEN 'Amazon DFS'
            WHEN a.source_no_ = 'BCN/2024/4064' OR d.name = 'Amazon FBA' THEN 'Amazon FBA'
            WHEN a.source_no_ = 'BCN/2021/0691' OR d.name IN ('SOUQ', 'Souq/Amazone') THEN 'Amazon'
            WHEN d.name = 'Project & Maintenance' THEN 'P&M'
            ELSE d.name
        END AS STRING) AS clc_global_dimension_2_code_name,
        
        -- Core Identifiers
        a.entry_no_,
        a.document_no_,
        a.document_line_no_,
        a.order_no_,
        a.order_line_no_,
        a.external_document_no_,

        -- Item Information
        a.item_no_,
        a.item_charge_no_,
        a.item_ledger_entry_no_,
        a.item_ledger_entry_quantity,
        a.variant_code,
        a.description,

        -- Transaction Types & Status
        a.document_type,
        a.order_type,
        a.entry_type,
        a.source_type,
        a.item_ledger_entry_type,
        a.variance_type,
        a.type,
        a.source_code,
        a.reason_code,
        a.return_reason_code,

        -- Dates
        a.document_date,
        a.posting_date,
        a.valuation_date,

        -- Quantities
        a.valued_quantity,
        a.invoiced_quantity,

        -- Financial Amounts (Local Currency)
        a.sales_amount__actual_,
        a.sales_amount__expected_,
        a.purchase_amount__actual_,
        a.purchase_amount__expected_,
        a.cost_amount__actual_,
        a.cost_amount__expected_,
        a.cost_amount__non_invtbl__,
        a.discount_amount,
        a.cost_per_unit,

        -- Financial Amounts (Additional Currency)
        a.cost_amount__actual___acy_,
        a.cost_amount__expected___acy_,
        a.cost_amount__non_invtbl___acy_,
        a.cost_per_unit__acy_,

        -- General Ledger Posting
        a.cost_posted_to_g_l,
        a.cost_posted_to_g_l__acy_,
        a.exp__cost_posted_to_g_l__acy_,
        a.expected_cost_posted_to_g_l,
        a.expected_cost,

        -- Posting Groups
        a.gen__prod__posting_group,
        a.gen__bus__posting_group,
        a.source_posting_group,
        a.inventory_posting_group,

        -- Dimensions
        a.global_dimension_1_code,
        a.global_dimension_2_code,
        a.dimension_set_id,

        -- Location & Logistics
        a.location_code,
        a.drop_shipment,

        -- Customer/Vendor Information
        a.source_no_,
        a.salespers__purch__code,

        -- Job/Project Related
        a.job_no_,
        a.job_task_no_,
        a.job_ledger_entry_no_,

        -- Journal Information
        a.journal_batch_name,

        -- Inventory Flags & Settings
        a.inventoriable,
        a.adjustment,
        a.applies_to_entry,
        a.average_cost_exception,
        a.partial_revaluation,
        a.valued_by_average_cost,

        -- Other References
        a.no_,
        a.capacity_ledger_entry_no_,

        -- System & Audit Fields
        a.user_id,
        a.timestamp,
        a._systemid,
        a._fivetran_deleted,
        a._fivetran_synced
        
    from {{ source('sql_erp_prod_dbo', 'petshop_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972') }} as a
    left join {{ ref('stg_dimension_value') }} as d on a.global_dimension_2_code = d.code and d.global_dimension_no_ = 2
),

-- Pethaus source
source_pethaus as (
    select
       'pethaus' AS company_source,
        a.dimension_code,
        a.global_dimension_2_code_name,
        a.clc_global_dimension_2_code_name,
        
        -- Core Identifiers
        a.entry_no_,
        a.document_no_,
        a.document_line_no_,
        a.order_no_,
        a.order_line_no_,
        a.external_document_no_,

        -- Item Information
        a.item_no_,
        a.item_charge_no_,
        a.item_ledger_entry_no_,
        a.item_ledger_entry_quantity,
        a.variant_code,
        a.description,

        -- Transaction Types & Status
        a.document_type,
        a.order_type,
        a.entry_type,
        a.source_type,
        a.item_ledger_entry_type,
        a.variance_type,
        a.type,
        a.source_code,
        a.reason_code,
        a.return_reason_code,

        -- Dates
        a.document_date,
        a.posting_date,
        a.valuation_date,

        -- Quantities
        a.valued_quantity,
        a.invoiced_quantity,

        -- Financial Amounts (Local Currency)
        a.sales_amount__actual_,
        a.sales_amount__expected_,
        a.purchase_amount__actual_,
        a.purchase_amount__expected_,
        a.cost_amount__actual_,
        a.cost_amount__expected_,
        a.cost_amount__non_invtbl__,
        a.discount_amount,
        a.cost_per_unit,

        -- Financial Amounts (Additional Currency)
        a.cost_amount__actual___acy_,
        a.cost_amount__expected___acy_,
        a.cost_amount__non_invtbl___acy_,
        a.cost_per_unit__acy_,

        -- General Ledger Posting
        a.cost_posted_to_g_l,
        a.cost_posted_to_g_l__acy_,
        a.exp__cost_posted_to_g_l__acy_,
        a.expected_cost_posted_to_g_l,
        a.expected_cost,

        -- Posting Groups
        a.gen__prod__posting_group,
        a.gen__bus__posting_group,
        a.source_posting_group,
        a.inventory_posting_group,

        -- Dimensions
        a.global_dimension_1_code,
        a.global_dimension_2_code,
        a.dimension_set_id,

        -- Location & Logistics
        a.location_code,
        a.drop_shipment,

        -- Customer/Vendor Information
        a.source_no_,
        a.salespers__purch__code,

        -- Job/Project Related
        a.job_no_,
        a.job_task_no_,
        a.job_ledger_entry_no_,

        -- Journal Information
        a.journal_batch_name,

        -- Inventory Flags & Settings
        a.inventoriable,
        a.adjustment,
        a.applies_to_entry,
        a.average_cost_exception,
        a.partial_revaluation,
        a.valued_by_average_cost,

        -- Other References
        a.no_,
        a.capacity_ledger_entry_no_,

        -- System & Audit Fields
        a.user_id,
        a.timestamp,
        a._systemid,
        a._fivetran_deleted,
        a._fivetran_synced
        
    from {{ ref('stg_pethaus_value_entry') }} as a
),

-- Union both sources
unioned as (
    select * from source_petshop
    
    UNION ALL
    
    select * from source_pethaus
),

renamed as (
    select
        company_source,
        dimension_code,
        global_dimension_2_code_name,
        clc_global_dimension_2_code_name,

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
        item_ledger_entry_type,
        variance_type,
        type,
        source_code,
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
        _fivetran_synced
    
    from unioned
)

select * from renamed

