with 
source as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_trans__sales_entry_5ecfc871_5d82_43f1_9c54_59685e82318d') }}
),

renamed as (
    select
            -- CALCULATED FIELDS
        case 
            when retail_product_code = '31024' then 'Add-on'
            when retail_product_code = '31010' then 'Bird Groom'
            when retail_product_code = '31011' then 'Cat Groom'
            when retail_product_code = '31012' then 'Dog Groom'
            when retail_product_code = '31113' then 'Mobile Cat'
            when retail_product_code = '31114' then 'Mobile Dog'
            else retail_product_code
        end as retail_product_code_2,

        -- PRIMARY IDENTIFIERS
        CONCAT(store_no_, '-', pos_terminal_no_, '-', transaction_no_) AS document_no_,
        store_no_,
        pos_terminal_no_,
        transaction_no_,
        line_no_,
        receipt_no_,
        
        -- ITEM INFORMATION
        item_no_,
        parent_item_no_,
        variant_code,
        barcode_no_,
        item_category_code,
        retail_product_code,
        item_posting_group,
        item_disc__group,
        price_group_code,
        
        -- CUSTOMER INFORMATION
        customer_no_,
        
        -- DATE & TIME FIELDS
        date,
        trans__date,
        time,
        trans__time,
        shift_date,
        shift_no_,
        expiration_date,
        
        -- QUANTITY & UOM
        quantity,
        refund_qty_,
        uom_quantity,
        unit_of_measure,
        infocode_selected_qty_,
        
        -- PRICING & AMOUNTS
        price,
        net_price,
        standard_net_price,
        uom_price,
        net_amount,
        cost_amount,
        vat_amount,
        total_rounded_amt_,
        sales_tax_rounding,
        
        -- DISCOUNTS
        discount_amount,
        customer_discount,
        cust__invoice_discount,
        line_discount,
        total_discount,
        periodic_discount,
        infocode_discount,
        coupon_discount,
        disc__amount_from_std__price,
        discount_amt__for_printing,
        coupon_amt__for_printing,
        line_was_discounted,
        total_disc__,
        
        -- DEAL & PROMOTION FIELDS
        promotion_no_,
        deal_line,
        deal_line_no_,
        deal_header_line_no_,
        deal_line_added_amt_,
        deal_modifier_line_no_,
        deal_modifier_added_amt_,
        periodic_disc__group,
        periodic_disc__type,
        
        -- TRANSACTION TYPE & STATUS
        transaction_code,
        sales_type,
        type_of_sale,
        xtransaction_status,
        return_no_sale,
        item_corrected_line,
        price_change,
        
        -- STAFF INFORMATION
        staff_id,
        sales_staff,
        created_by_staff_id,
        
        -- ITEM ENTRY METHODS
        keyboard_item_entry,
        item_number_scanned,
        scale_item,
        weight_item,
        weight_manually_entered,
        price_in_barcode,
        recommended_item,
        
        -- LINKED & REFERENCE LINES
        parent_line_no_,
        orig_trans_line_no_,
        orig_trans_no_,
        orig_trans_pos,
        orig_trans_store,
        refunded_line_no_,
        refunded_trans__no_,
        refunded_pos_no_,
        refunded_store_no_,
        linked_no__not_orig_,
        orig__of_a_linked_item_list,
        excluded_bom_line_no_,
        
        -- INFOCODE FIELDS
        infocode_entry_line_no_,
        orig__from_infocode,
        orig__from_subcode,
        tot__disc_info_line_no_,
        
        -- INVENTORY & TRACKING
        lot_no_,
        serial_no_,
        serial_lot_no__not_valid,
        section,
        shelf,
        
        -- LOYALTY & POINTS
        member_points,
        member_points_type,
        offer_blocked_points,
        
        -- TAX & VAT FIELDS
        tax_group_code,
        vat_code,
        vat_bus__posting_group,
        vat_calculation_type,
        
        -- GIFT & SPECIAL FEATURES
        marked_for_gift_receipt,
        
        -- ACCOUNTING & POSTING
        statement_code,
        xstatement_no_,
        posting_exception_key,
        
        -- REPLICATION & SYNC
        replicated,
        replication_counter,
        counter,
        
        -- SYSTEM FIELDS
        _systemid,
        timestamp,
        bi_timestamp,
        _fivetran_synced,
        _fivetran_deleted,
        

    from source
)

select * from renamed

--where CONCAT(store_no_, '-', pos_terminal_no_, '-', transaction_no_) = 'DIP-DT01-132998'

--where receipt_no_ = '000000CK02000008408'
--where transaction_no_ = 8575

--WHERE retail_product_code IN ('31114')

--WHERE retail_product_code NOT IN ('31024','31010','31011','31012','31113','31114')


--when retail_product_code = '31024' then 'Add-on'
--when retail_product_code = '31010' then 'Bird Groom' NOOOOO Data

--when retail_product_code = '31012' then 'Dog Groom' --GRM-GR01-4354
--when retail_product_code = '31011' then 'Cat Groom' --GRM-GR01-4781

--when retail_product_code = '31113' then 'Mobile Cat' --MOBILE-GR05-3217
--when retail_product_code = '31114' then 'Mobile Dog' --MOBILE-GR05-3217



