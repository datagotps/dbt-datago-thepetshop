with 

source_1 as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_purchase_line_437dbf0e_84ff_417a_965d_ed2bb9650972') }}
),

source_2 as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_purchase_line_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}
),

renamed_source_1 as (
    select
        document_no_,
        receipt_no_,
        document_type,
        line_no_,
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        a__rcd__not_inv__ex__vat__lcy_,
        allow_invoice_disc_,
        allow_item_charge_assignment,
        amount,
        amount_including_vat,
        amt__rcd__not_invoiced,
        amt__rcd__not_invoiced__lcy_,
        appl__to_item_entry,
        area,
        attached_to_line_no_,
        bin_code,
        blanket_order_line_no_,
        blanket_order_no_,
        budgeted_fa_no_,
        buy_from_vendor_no_,
        completely_received,
        copied_from_posted_doc_,
        cross_reference_no_,
        cross_reference_type,
        cross_reference_type_no_,
        currency_code,
        deferral_code,
        depr__acquisition_cost,
        depr__until_fa_posting_date,
        depreciation_book_code,
        description,
        description_2,
        dimension_set_id,
        direct_unit_cost,
        drop_shipment,
        duplicate_in_depreciation_book,
        entry_point,
        expected_receipt_date,
        fa_posting_date,
        fa_posting_type,
        finished,
        gen__bus__posting_group,
        gen__prod__posting_group,
        gross_weight,
        ic_partner_code,
        ic_partner_ref__type,
        ic_partner_reference,
        inbound_whse__handling_time,
        indirect_cost__,
        insurance_no_,
        inv__disc__amount_to_invoice,
        inv__discount_amount,
        item_category_code,
        job_currency_code,
        job_currency_factor,
        job_line_amount,
        job_line_amount__lcy_,
        job_line_disc__amount__lcy_,
        job_line_discount__,
        job_line_discount_amount,
        job_line_type,
        job_no_,
        job_planning_line_no_,
        job_remaining_qty_,
        job_remaining_qty___base_,
        job_task_no_,
        job_total_price,
        job_total_price__lcy_,
        job_unit_price,
        job_unit_price__lcy_,
        lead_time_calculation,
        line_amount,
        line_discount__,
        line_discount_amount,
        location_code,
        maintenance_code,
        mps_order,
        net_weight,
        no_,
        nonstock,
        operation_no_,
        order_date,
        order_line_no_,
        order_no_,
        outstanding_amount,
        outstanding_amount__lcy_,
        outstanding_amt__ex__vat__lcy_,
        outstanding_qty___base_,
        outstanding_quantity,
        over_receipt_approval_status,
        over_receipt_code,
        over_receipt_quantity,
        overhead_rate,
        pay_to_vendor_no_,
        planned_receipt_date,
        planning_flexibility,
        pmt__discount_amount,
        posting_group,
        prepayment__,
        prepayment_amount,
        prepayment_line,
        prepayment_tax_area_code,
        prepayment_tax_group_code,
        prepayment_tax_liable,
        prepayment_vat__,
        prepayment_vat_difference,
        prepayment_vat_identifier,
        prepmt__amount_inv___lcy_,
        prepmt__amount_inv__incl__vat,
        prepmt__amt__incl__vat,
        prepmt__amt__inv_,
        prepmt__line_amount,
        prepmt__vat_amount_inv___lcy_,
        prepmt__vat_base_amt_,
        prepmt__vat_calc__type,
        prepmt_amt_deducted,
        prepmt_amt_to_deduct,
        prepmt_vat_diff__deducted,
        prepmt_vat_diff__to_deduct,
        price_calculation_method,
        prod__order_line_no_,
        prod__order_no_,
        product_group_code,
        profit__,
        promised_receipt_date,
        purchasing_code,
        qty__invoiced__base_,
        qty__per_unit_of_measure,
        qty__rcd__not_invoiced,
        qty__rcd__not_invoiced__base_,
        qty__received__base_,
        qty__to_invoice,
        qty__to_invoice__base_,
        qty__to_receive,
        qty__to_receive__base_,
        quantity,
        quantity__base_,
        quantity_invoiced,
        quantity_received,
        recalculate_invoice_disc_,
        receipt_line_no_,
        requested_receipt_date,
        responsibility_center,
        ret__qty__shpd_not_invd__base_,
        return_qty__shipped,
        return_qty__shipped__base_,
        return_qty__shipped_not_invd_,
        return_qty__to_ship,
        return_qty__to_ship__base_,
        return_reason_code,
        return_shipment_line_no_,
        return_shipment_no_,
        return_shpd__not_invd_,
        return_shpd__not_invd___lcy_,
        returns_deferral_start_date,
        routing_no_,
        routing_reference_no_,
        safety_lead_time,
        sales_order_line_no_,
        sales_order_no_,
        salvage_value,
        shortcut_dimension_1_code,
        shortcut_dimension_2_code,
        special_order,
        special_order_sales_line_no_,
        special_order_sales_no_,
        subtype,
        system_created_entry,
        tax_area_code,
        tax_group_code,
        tax_liable,
        timestamp,
        transaction_specification,
        transaction_type,
        transport_method,
        type,
        unit_cost,
        unit_cost__lcy_,
        unit_of_measure,
        unit_of_measure__cross_ref__,
        unit_of_measure_code,
        unit_price__lcy_,
        unit_volume,
        units_per_parcel,
        use_duplication_list,
        use_tax,
        variant_code,
        vat__,
        vat_base_amount,
        vat_bus__posting_group,
        vat_calculation_type,
        vat_difference,
        vat_identifier,
        vat_prod__posting_group,
        vendor_item_no_,
        work_center_no_
    from source_1
),

renamed_source_2 as (
    select
        document_no_,
        document_type,
        line_no_,
        _fivetran_deleted,
        _fivetran_synced,
        failed_quantity,
        inventory_type,
        passed_quantity,
        qc_done,
        quality_status,
        timestamp,
        fail_quantity,
        qc_by,
        pass_quantity
    from source_2
),

joined as (
    select
        -- ===================================
        -- PRIMARY KEYS & IDENTIFIERS
        -- ===================================
        s1.document_no_,
        s1.document_type,
        s1.line_no_,
        s1.receipt_no_,
        s1.order_no_,
        s1.order_line_no_,
        s1._systemid,

        -- ===================================
        -- METADATA & SYSTEM FIELDS
        -- ===================================
        s1._fivetran_deleted,
        s1._fivetran_synced,
        s1.timestamp,
        s1.system_created_entry,
        s1.copied_from_posted_doc_,

        -- ===================================
        -- ITEM INFORMATION
        -- ===================================
        s1.no_,
        s1.type,
        s1.description,
        s1.description_2,
        s1.variant_code,
        s1.item_category_code,
        s1.product_group_code,
        s1.vendor_item_no_,
        s1.nonstock,

        -- ===================================
        -- VENDOR INFORMATION
        -- ===================================
        s1.buy_from_vendor_no_,
        s1.pay_to_vendor_no_,

        -- ===================================
        -- QUANTITY FIELDS
        -- ===================================
        s1.quantity, --Original order quantity requested from vendor
        s1.quantity__base_,
        s1.quantity_received,
        s1.quantity_invoiced,
        s1.outstanding_quantity,
        s1.outstanding_qty___base_,
        s1.qty__to_receive,
        s1.qty__to_receive__base_,
        s1.qty__to_invoice,
        s1.qty__to_invoice__base_,
        s1.qty__received__base_,
        s1.qty__invoiced__base_,
        s1.qty__rcd__not_invoiced,
        s1.qty__rcd__not_invoiced__base_,
        s1.qty__per_unit_of_measure,
        s1.over_receipt_quantity,
        s1.completely_received,

        -- ===================================
        -- RETURN QUANTITIES
        -- ===================================
        s1.return_qty__to_ship,
        s1.return_qty__to_ship__base_,
        s1.return_qty__shipped,
        s1.return_qty__shipped__base_,
        s1.return_qty__shipped_not_invd_,
        s1.ret__qty__shpd_not_invd__base_,
        s1.return_reason_code,
        s1.return_shipment_no_,
        s1.return_shipment_line_no_,

        -- ===================================
        -- UNIT OF MEASURE
        -- ===================================
        s1.unit_of_measure,
        s1.unit_of_measure_code,
        s1.unit_of_measure__cross_ref__,

        -- ===================================
        -- PRICING & COSTS
        -- ===================================
        s1.direct_unit_cost,
        s1.unit_cost,
        s1.unit_cost__lcy_,
        s1.unit_price__lcy_,
        s1.line_amount,
        s1.indirect_cost__,
        s1.overhead_rate,
        s1.profit__,
        s1.price_calculation_method,

        -- ===================================
        -- AMOUNTS & FINANCIAL
        -- ===================================
        s1.amount,
        s1.amount_including_vat,
        s1.outstanding_amount,
        s1.outstanding_amount__lcy_,
        s1.outstanding_amt__ex__vat__lcy_,
        s1.amt__rcd__not_invoiced,
        s1.amt__rcd__not_invoiced__lcy_,
        s1.a__rcd__not_inv__ex__vat__lcy_,
        s1.currency_code,

        -- ===================================
        -- DISCOUNTS
        -- ===================================
        s1.line_discount__,
        s1.line_discount_amount,
        s1.inv__discount_amount,
        s1.inv__disc__amount_to_invoice,
        s1.pmt__discount_amount,
        s1.allow_invoice_disc_,
        s1.recalculate_invoice_disc_,

        -- ===================================
        -- VAT & TAX
        -- ===================================
        s1.vat__,
        s1.vat_base_amount,
        s1.vat_calculation_type,
        s1.vat_identifier,
        s1.vat_difference,
        s1.vat_bus__posting_group,
        s1.vat_prod__posting_group,
        s1.tax_area_code,
        s1.tax_group_code,
        s1.tax_liable,
        s1.use_tax,

        -- ===================================
        -- PREPAYMENT
        -- ===================================
        s1.prepayment__,
        s1.prepayment_amount,
        s1.prepayment_line,
        s1.prepayment_vat__,
        s1.prepayment_vat_difference,
        s1.prepayment_vat_identifier,
        s1.prepayment_tax_area_code,
        s1.prepayment_tax_group_code,
        s1.prepayment_tax_liable,
        s1.prepmt__line_amount,
        s1.prepmt__amt__incl__vat,
        s1.prepmt__amt__inv_,
        s1.prepmt__amount_inv__incl__vat,
        s1.prepmt__amount_inv___lcy_,
        s1.prepmt__vat_base_amt_,
        s1.prepmt__vat_amount_inv___lcy_,
        s1.prepmt__vat_calc__type,
        s1.prepmt_amt_to_deduct,
        s1.prepmt_amt_deducted,
        s1.prepmt_vat_diff__to_deduct,
        s1.prepmt_vat_diff__deducted,

        -- ===================================
        -- DATES
        -- ===================================
        s1.order_date,
        s1.expected_receipt_date,
        s1.promised_receipt_date,
        s1.requested_receipt_date,
        s1.planned_receipt_date,
        s1.returns_deferral_start_date,
        s1.fa_posting_date,

        -- ===================================
        -- LOCATION & WAREHOUSE
        -- ===================================
        s1.location_code,
        s1.bin_code,
        s1.area,
        s1.inbound_whse__handling_time,

        -- ===================================
        -- POSTING GROUPS & DIMENSIONS
        -- ===================================
        s1.gen__bus__posting_group,
        s1.gen__prod__posting_group,
        s1.posting_group,
        s1.dimension_set_id,
        s1.shortcut_dimension_1_code,
        s1.shortcut_dimension_2_code,
        s1.responsibility_center,

        -- ===================================
        -- SHIPPING & LOGISTICS
        -- ===================================
        s1.gross_weight,
        s1.net_weight,
        s1.unit_volume,
        s1.units_per_parcel,
        s1.transport_method,
        s1.entry_point,
        s1.transaction_type,
        s1.transaction_specification,

        -- ===================================
        -- SPECIAL ORDER & DROP SHIPMENT
        -- ===================================
        s1.drop_shipment,
        s1.special_order,
        s1.special_order_sales_no_,
        s1.special_order_sales_line_no_,
        s1.sales_order_no_,
        s1.sales_order_line_no_,

        -- ===================================
        -- BLANKET ORDER
        -- ===================================
        s1.blanket_order_no_,
        s1.blanket_order_line_no_,

        -- ===================================
        -- PRODUCTION ORDER
        -- ===================================
        s1.prod__order_no_,
        s1.prod__order_line_no_,
        s1.routing_no_,
        s1.routing_reference_no_,
        s1.operation_no_,
        s1.work_center_no_,

        -- ===================================
        -- JOB/PROJECT COSTING
        -- ===================================
        s1.job_no_,
        s1.job_task_no_,
        s1.job_line_type,
        s1.job_planning_line_no_,
        s1.job_unit_price,
        s1.job_unit_price__lcy_,
        s1.job_line_amount,
        s1.job_line_amount__lcy_,
        s1.job_total_price,
        s1.job_total_price__lcy_,
        s1.job_line_discount__,
        s1.job_line_discount_amount,
        s1.job_line_disc__amount__lcy_,
        s1.job_remaining_qty_,
        s1.job_remaining_qty___base_,
        s1.job_currency_code,
        s1.job_currency_factor,

        -- ===================================
        -- FIXED ASSETS
        -- ===================================
        s1.budgeted_fa_no_,
        s1.fa_posting_type,
       -- s1.fa_posting_date,
        s1.depreciation_book_code,
        s1.depr__acquisition_cost,
        s1.depr__until_fa_posting_date,
        s1.salvage_value,
        s1.maintenance_code,
        s1.insurance_no_,
        s1.duplicate_in_depreciation_book,
        s1.use_duplication_list,

        -- ===================================
        -- PLANNING & MRP
        -- ===================================
        s1.lead_time_calculation,
        s1.safety_lead_time,
        s1.planning_flexibility,
        s1.mps_order,

        -- ===================================
        -- CROSS REFERENCE
        -- ===================================
        s1.cross_reference_no_,
        s1.cross_reference_type,
        s1.cross_reference_type_no_,

        -- ===================================
        -- INTERCOMPANY
        -- ===================================
        s1.ic_partner_code,
        s1.ic_partner_ref__type,
        s1.ic_partner_reference,

        -- ===================================
        -- ITEM CHARGES & DEFERRALS
        -- ===================================
        s1.allow_item_charge_assignment,
        s1.attached_to_line_no_,
        s1.deferral_code,

        -- ===================================
        -- RECEIPT & INVOICE LINES
        -- ===================================
        s1.receipt_line_no_,
        s1.appl__to_item_entry,

        -- ===================================
        -- RETURNS
        -- ===================================
        s1.return_shpd__not_invd_,
        s1.return_shpd__not_invd___lcy_,

        -- ===================================
        -- APPROVAL & STATUS
        -- ===================================
        s1.over_receipt_approval_status,
        s1.over_receipt_code,
        s1.finished,

        -- ===================================
        -- OTHER
        -- ===================================
        s1.purchasing_code,
        s1.subtype,
        
        -- ===================================
        -- QUALITY CONTROL (from source_2)
        -- ===================================
        s2.qc_done,
        s2.quality_status,
        s2.qc_by,
        s2.passed_quantity,
        s2.failed_quantity,
        s2.pass_quantity,
        s2.fail_quantity,
        s2.inventory_type

    from renamed_source_1 s1
    left join renamed_source_2 s2
        on s1.document_no_ = s2.document_no_
        and s1.document_type = s2.document_type
        and s1.line_no_ = s2.line_no_
)

select * from joined

--where  document_no_ = 'TPS/2025/001411'  
