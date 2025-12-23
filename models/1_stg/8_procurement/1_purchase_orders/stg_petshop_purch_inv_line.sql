-- ══════════════════════════════════════════════════════════════════════════════
-- stg_petshop_purch_inv_line.sql
-- Purpose: Staging model for Posted Purchase Invoice Lines
-- Source: BC/NAV ERP - Purch. Inv. Line table
-- Description: Contains line-level details of posted (finalized) purchase invoices
-- ══════════════════════════════════════════════════════════════════════════════

with 

source as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_purch__inv__line_437dbf0e_84ff_417a_965d_ed2bb9650972') }}
),

renamed as (
    select
        -- ══════════════════════════════════════════════════════════════════════
        -- PRIMARY KEYS / IDENTIFIERS
        -- ══════════════════════════════════════════════════════════════════════
        document_no_,                         -- Posted Invoice Number (e.g., PPI/053201)
        line_no_,                             -- Line number within invoice
        order_no_,                            -- Original PO Number (link to PO)
        order_line_no_,                       -- Original PO Line Number
        receipt_no_,                          -- GRN Number (if applicable)
        receipt_line_no_,                     -- GRN Line Number

        -- ══════════════════════════════════════════════════════════════════════
        -- ITEM INFORMATION
        -- ══════════════════════════════════════════════════════════════════════
        no_ as item_no,                       -- Item Number
        description,                          -- Item Description
        description_2,                        -- Extended Description
        item_category_code,                   -- Item Category
        type,                                 -- Line Type (2=Item)
        variant_code,                         -- Item Variant Code
        vendor_item_no_,                      -- Vendor's Item Number

        -- ══════════════════════════════════════════════════════════════════════
        -- VENDOR INFORMATION
        -- ══════════════════════════════════════════════════════════════════════
        buy_from_vendor_no_,                  -- Vendor Code
        pay_to_vendor_no_,                    -- Pay-to Vendor Code

        -- ══════════════════════════════════════════════════════════════════════
        -- DATES
        -- ══════════════════════════════════════════════════════════════════════
        posting_date,                         -- Invoice Posting Date
        expected_receipt_date,                -- Expected Receipt Date (from PO)

        -- ══════════════════════════════════════════════════════════════════════
        -- QUANTITIES
        -- ══════════════════════════════════════════════════════════════════════
        quantity,                             -- Invoiced Quantity
        quantity__base_ as quantity_base,     -- Quantity in Base UOM
        qty__per_unit_of_measure,             -- UOM Conversion Factor

        -- ══════════════════════════════════════════════════════════════════════
        -- UNIT OF MEASURE
        -- ══════════════════════════════════════════════════════════════════════
        unit_of_measure_code,                 -- UOM Code
        unit_of_measure,                      -- UOM Description

        -- ══════════════════════════════════════════════════════════════════════
        -- PRICING & COSTS (Gross → Discount → Net)
        -- ══════════════════════════════════════════════════════════════════════
        direct_unit_cost,                     -- Unit Cost from Vendor
        unit_cost,                            -- Unit Cost (may include overhead)
        unit_cost__lcy_ as unit_cost_lcy,     -- Unit Cost in Local Currency (AED)
        
        -- Gross Value (before discount)
        (quantity * direct_unit_cost) as inv_gross_value,
        
        -- Discount
        line_discount__ as inv_discount_pct,  -- Line Discount Percentage
        line_discount_amount as inv_discount_amount,  -- Line Discount Amount
        inv__discount_amount as invoice_discount_amount,  -- Invoice-Level Discount
        
        -- Net Value (after discount)
        line_amount as inv_line_amount,       -- Line Amount (Qty × Unit Cost - Discount)
        amount as inv_net_value,              -- Net Amount (excl. VAT)
        
        -- VAT
        vat__ as vat_pct,                     -- VAT Percentage
        vat_base_amount,                      -- VAT Base Amount
        amount_including_vat as inv_total_value,  -- Total Amount incl. VAT

        -- ══════════════════════════════════════════════════════════════════════
        -- POSTING GROUPS
        -- ══════════════════════════════════════════════════════════════════════
        gen__bus__posting_group,              -- General Business Posting Group
        gen__prod__posting_group,             -- General Product Posting Group
        vat_bus__posting_group,               -- VAT Business Posting Group
        vat_prod__posting_group,              -- VAT Product Posting Group
        posting_group,                        -- Inventory Posting Group
        vat_identifier,                       -- VAT Identifier
        vat_calculation_type,                 -- VAT Calculation Type

        -- ══════════════════════════════════════════════════════════════════════
        -- LOCATION / LOGISTICS
        -- ══════════════════════════════════════════════════════════════════════
        location_code,                        -- Location/Warehouse Code
        bin_code,                             -- Bin Code
        shortcut_dimension_1_code,            -- Dimension 1 (e.g., Location)
        shortcut_dimension_2_code,            -- Dimension 2
        dimension_set_id,                     -- Dimension Set ID

        -- ══════════════════════════════════════════════════════════════════════
        -- BLANKET ORDER REFERENCE
        -- ══════════════════════════════════════════════════════════════════════
        blanket_order_no_,                    -- Blanket Order Number
        blanket_order_line_no_,               -- Blanket Order Line Number

        -- ══════════════════════════════════════════════════════════════════════
        -- CROSS REFERENCE
        -- ══════════════════════════════════════════════════════════════════════
        cross_reference_no_,                  -- Cross Reference Number
        cross_reference_type,                 -- Cross Reference Type
        cross_reference_type_no_,             -- Cross Reference Type Number

        -- ══════════════════════════════════════════════════════════════════════
        -- JOB COSTING (if applicable)
        -- ══════════════════════════════════════════════════════════════════════
        job_no_,                              -- Job Number
        job_task_no_,                         -- Job Task Number
        job_line_type,                        -- Job Line Type
        job_unit_price,                       -- Job Unit Price
        job_unit_price__lcy_ as job_unit_price_lcy,
        job_total_price,                      -- Job Total Price
        job_total_price__lcy_ as job_total_price_lcy,
        job_line_amount,                      -- Job Line Amount
        job_line_amount__lcy_ as job_line_amount_lcy,
        job_line_discount__,                  -- Job Line Discount %
        job_line_discount_amount,             -- Job Line Discount Amount
        job_line_disc__amount__lcy_ as job_line_discount_amount_lcy,
        job_currency_code,                    -- Job Currency Code
        job_currency_factor,                  -- Job Currency Factor

        -- ══════════════════════════════════════════════════════════════════════
        -- FIXED ASSET (if applicable)
        -- ══════════════════════════════════════════════════════════════════════
        fa_posting_date,                      -- FA Posting Date
        fa_posting_type,                      -- FA Posting Type
        depreciation_book_code,               -- Depreciation Book Code
        depr__acquisition_cost,               -- Depreciation Acquisition Cost
        depr__until_fa_posting_date,          -- Depreciate Until FA Posting Date
        budgeted_fa_no_,                      -- Budgeted FA Number
        duplicate_in_depreciation_book,       -- Duplicate in Depreciation Book
        use_duplication_list,                 -- Use Duplication List
        salvage_value,                        -- Salvage Value
        insurance_no_,                        -- Insurance Number
        maintenance_code,                     -- Maintenance Code

        -- ══════════════════════════════════════════════════════════════════════
        -- PRODUCTION ORDER (if applicable)
        -- ══════════════════════════════════════════════════════════════════════
        prod__order_no_ as prod_order_no,     -- Production Order Number
        prod__order_line_no_ as prod_order_line_no,  -- Production Order Line
        operation_no_,                        -- Operation Number
        work_center_no_,                      -- Work Center Number
        routing_no_,                          -- Routing Number
        routing_reference_no_,                -- Routing Reference Number

        -- ══════════════════════════════════════════════════════════════════════
        -- OTHER COSTS
        -- ══════════════════════════════════════════════════════════════════════
        indirect_cost__ as indirect_cost_pct, -- Indirect Cost Percentage
        overhead_rate,                        -- Overhead Rate
        pmt__discount_amount as payment_discount_amount,  -- Payment Discount

        -- ══════════════════════════════════════════════════════════════════════
        -- ITEM ATTRIBUTES
        -- ══════════════════════════════════════════════════════════════════════
        gross_weight,                         -- Gross Weight
        net_weight,                           -- Net Weight
        unit_volume,                          -- Unit Volume
        units_per_parcel,                     -- Units Per Parcel

        -- ══════════════════════════════════════════════════════════════════════
        -- TAX
        -- ══════════════════════════════════════════════════════════════════════
        tax_area_code,                        -- Tax Area Code
        tax_group_code,                       -- Tax Group Code
        tax_liable,                           -- Tax Liable Flag
        use_tax,                              -- Use Tax Flag

        -- ══════════════════════════════════════════════════════════════════════
        -- INTRASTAT / CUSTOMS
        -- ══════════════════════════════════════════════════════════════════════
        area,                                 -- Area Code
        entry_point,                          -- Entry Point
        transaction_specification,            -- Transaction Specification
        transaction_type,                     -- Transaction Type
        transport_method,                     -- Transport Method

        -- ══════════════════════════════════════════════════════════════════════
        -- IC PARTNER
        -- ══════════════════════════════════════════════════════════════════════
        ic_partner_code,                      -- IC Partner Code
        ic_partner_reference,                 -- IC Partner Reference
        ic_partner_ref__type,                 -- IC Partner Reference Type

        -- ══════════════════════════════════════════════════════════════════════
        -- OTHER FLAGS
        -- ══════════════════════════════════════════════════════════════════════
        allow_invoice_disc_,                  -- Allow Invoice Discount
        nonstock,                             -- Non-Stock Item Flag
        prepayment_line,                      -- Prepayment Line Flag
        attached_to_line_no_,                 -- Attached to Line Number
        system_created_entry,                 -- System Created Flag
        deferral_code,                        -- Deferral Code
        return_reason_code,                   -- Return Reason Code
        purchasing_code,                      -- Purchasing Code
        responsibility_center,                -- Responsibility Center
        appl__to_item_entry,                  -- Applied to Item Entry
        price_calculation_method,             -- Price Calculation Method

        -- ══════════════════════════════════════════════════════════════════════
        -- METADATA
        -- ══════════════════════════════════════════════════════════════════════
        _systemid,                            -- System ID (BC internal)
        timestamp,                            -- ERP Timestamp
        _fivetran_deleted,                    -- Soft Delete Flag
        _fivetran_synced                      -- Last Sync Timestamp

    from source
)

select * from renamed

