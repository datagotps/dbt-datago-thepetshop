with 

-- Petshop source
source_petshop as (
    select 
       'Pet Shop' AS company_source,
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
       'Pet Haus' AS company_source,
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

-- TPS Café source
source_Cafe as (
    select
       'TPS Café' AS company_source,
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
        
    from {{ ref('stg_petshop_cafe_value_entry') }} as a
),

-- Service Revenue source from trans_sales_entry with proper data type casting
source_service_revenue as (
    select
        CAST('Pet Shop Services' AS STRING) AS company_source,
        CAST('PROFITCENTER' AS STRING) as dimension_code,
        CAST(clc_store_no_ AS STRING) as global_dimension_2_code_name,
        CAST(clc_store_no_ AS STRING) as clc_global_dimension_2_code_name,
        /*
        CAST(CASE 
            WHEN retail_product_code = '31024' THEN 'Add-on'
            WHEN retail_product_code = '31010' THEN 'Bird Groom'
            WHEN retail_product_code = '31011' THEN 'Cat Groom'
            WHEN retail_product_code = '31012' THEN 'Dog Groom'
            WHEN retail_product_code = '31113' THEN 'Mobile Cat'
            WHEN retail_product_code = '31114' THEN 'Mobile Dog'
            ELSE retail_product_code
        END AS STRING) AS clc_global_dimension_2_code_name,
        */
        
        -- Core Identifiers
        CAST(line_no_ AS INTEGER) as entry_no_,
        document_no_,
        CAST(line_no_ AS INTEGER) as document_line_no_,
        CAST(transaction_no_ AS STRING) as order_no_,
        CAST(line_no_ AS INTEGER) as order_line_no_,
        CAST(receipt_no_ AS STRING) as external_document_no_,

        -- Item Information
        CAST(item_no_ AS STRING) as item_no_,
        CAST(NULL AS STRING) as item_charge_no_,
        CAST(NULL AS INTEGER) as item_ledger_entry_no_,
        CAST(quantity AS INT64) as item_ledger_entry_quantity,  -- Changed from BIGNUM to INT64
        CAST(variant_code AS STRING) as variant_code,
        CAST(CONCAT('Service: ', retail_product_code_2) AS STRING) as description,

        -- Transaction Types & Status
        CAST(0 AS INTEGER) as document_type,  -- 1 for Sales
        CAST(0 AS INTEGER) as order_type,
        CAST(CASE 
            WHEN transaction_code IS NOT NULL THEN transaction_code 
            ELSE 0 
        END AS INTEGER) as entry_type,
        CAST(1 AS INTEGER) as source_type,  -- 1 for Customer
        CAST(1 AS INTEGER) as item_ledger_entry_type,  -- 1 for Sale
        CAST(0 AS INTEGER) as variance_type,
        CAST(CASE 
            WHEN type_of_sale IS NOT NULL THEN type_of_sale 
            ELSE 0 
        END AS INTEGER) as type,
        CAST('BACKOFFICE' AS STRING) as source_code,
        CAST(NULL AS STRING) as reason_code,
        CAST(return_no_sale AS STRING) as return_reason_code,

        -- Dates
        CAST(date AS DATETIME) as document_date,
        CAST(trans__date AS DATETIME) as posting_date,
        CAST(trans__date AS DATETIME) as valuation_date,

        -- Quantities
        CAST(quantity AS BIGNUMERIC) as valued_quantity,
        CAST(quantity AS BIGNUMERIC) as invoiced_quantity,

        -- Financial Amounts (Local Currency)
        CAST(net_amount * -1 AS BIGNUMERIC) as sales_amount__actual_,
        CAST(net_amount AS BIGNUMERIC) as sales_amount__expected_,
        CAST(0 AS BIGNUMERIC) as purchase_amount__actual_,
        CAST(0 AS BIGNUMERIC) as purchase_amount__expected_,
       -- CAST(cost_amount AS BIGNUMERIC) as cost_amount__actual_,
        CASE WHEN item_category_code IN ('310', '311') THEN 0 ELSE cost_amount  END cost_amount__actual_,
        CAST(cost_amount AS BIGNUMERIC) as cost_amount__expected_,
        CAST(0 AS BIGNUMERIC) as cost_amount__non_invtbl__,
        CAST(discount_amount AS BIGNUMERIC) as discount_amount,
        CAST(CASE WHEN quantity <> 0 THEN cost_amount / quantity ELSE 0 END AS BIGNUMERIC) as cost_per_unit,

        -- Financial Amounts (Additional Currency)
        CAST(cost_amount AS BIGNUMERIC) as cost_amount__actual___acy_,
        CAST(cost_amount AS BIGNUMERIC) as cost_amount__expected___acy_,
        CAST(0 AS BIGNUMERIC) as cost_amount__non_invtbl___acy_,
        CAST(CASE WHEN quantity <> 0 THEN cost_amount / quantity ELSE 0 END AS BIGNUMERIC) as cost_per_unit__acy_,

        -- General Ledger Posting
        CAST(0 AS BIGNUMERIC) as cost_posted_to_g_l,
        CAST(0 AS BIGNUMERIC) as cost_posted_to_g_l__acy_,
        CAST(0 AS BIGNUMERIC) as exp__cost_posted_to_g_l__acy_,
        CAST(0 AS BIGNUMERIC) as expected_cost_posted_to_g_l,
        CAST(0 AS INTEGER) as expected_cost,

        -- Posting Groups
        CAST(item_posting_group AS STRING) as gen__prod__posting_group,
        CAST(vat_bus__posting_group AS STRING) as gen__bus__posting_group,
        CAST(NULL AS STRING) as source_posting_group,
        CAST(NULL AS STRING) as inventory_posting_group,

        -- Dimensions
        CAST(store_no_ AS STRING) as global_dimension_1_code,
        CAST('PETGROOM' AS STRING) as global_dimension_2_code,
        CAST(NULL AS INTEGER) as dimension_set_id,

        -- Location & Logistics
        CAST(store_no_ AS STRING) as location_code,
        CAST(0 AS INTEGER) as drop_shipment,

        -- Customer/Vendor Information
        CAST(customer_no_ AS STRING) as source_no_,
        CAST(sales_staff AS STRING) as salespers__purch__code,

        -- Job/Project Related
        CAST(NULL AS STRING) as job_no_,
        CAST(NULL AS STRING) as job_task_no_,
        CAST(NULL AS INTEGER) as job_ledger_entry_no_,

        -- Journal Information
        CAST(pos_terminal_no_ AS STRING) as journal_batch_name,

        -- Inventory Flags & Settings
        CAST(0 AS INTEGER) as inventoriable,
        CAST(0 AS INTEGER) as adjustment,
        CAST(NULL AS INTEGER) as applies_to_entry,
        CAST(0 AS INTEGER) as average_cost_exception,
        CAST(0 AS INTEGER) as partial_revaluation,
        CAST(0 AS INTEGER) as valued_by_average_cost,

        -- Other References
        CAST(transaction_no_ AS STRING) as no_,
        CAST(NULL AS INTEGER) as capacity_ledger_entry_no_,

        -- System & Audit Fields
        CAST(staff_id AS STRING) as user_id,
        timestamp,  -- BYTES type, no casting needed
        CAST(_systemid AS STRING) as _systemid,
        _fivetran_deleted,  -- BOOLEAN type, no casting needed
        _fivetran_synced  -- TIMESTAMP type, no casting needed
        
    from {{ ref('int_erp_trans__sales_entry') }}
),

-- Union all sources
unioned as (
    select * from source_petshop
    
    UNION ALL
    
    select * from source_pethaus

    UNION ALL

    select * from source_Cafe
    
    UNION ALL
    
    select * from source_service_revenue
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
