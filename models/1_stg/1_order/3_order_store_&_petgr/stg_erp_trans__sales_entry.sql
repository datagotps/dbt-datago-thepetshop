with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_trans__sales_entry_5ecfc871_5d82_43f1_9c54_59685e82318d') }}

),

renamed as (

    select
        
        
        item_no_,
        item_category_code,
        retail_product_code,
        customer_no_,
        date,
        transaction_no_,
        store_no_,
        quantity,
        discount_amount,
        net_amount,
        cost_amount,

        vat_amount,
        disc__amount_from_std__price,

        
        
        



        line_no_,
        pos_terminal_no_,
        
        
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        barcode_no_,
        bi_timestamp,
        
        counter,
        coupon_amt__for_printing,
        coupon_discount,
        created_by_staff_id,
        cust__invoice_discount,
        customer_discount,
        
        
        deal_header_line_no_,
        deal_line,
        deal_line_added_amt_,
        deal_line_no_,
        deal_modifier_added_amt_,
        deal_modifier_line_no_,
        
        
        discount_amt__for_printing,
        excluded_bom_line_no_,
        expiration_date,
        infocode_discount,
        infocode_entry_line_no_,
        infocode_selected_qty_,
        
        item_corrected_line,
        item_disc__group,
        
        item_number_scanned,
        item_posting_group,
        keyboard_item_entry,
        line_discount,
        line_was_discounted,
        linked_no__not_orig_,
        lot_no_,
        marked_for_gift_receipt,
        member_points,
        member_points_type,
        
        net_price,
        offer_blocked_points,
        orig__from_infocode,
        orig__from_subcode,
        orig__of_a_linked_item_list,
        orig_trans_line_no_,
        orig_trans_no_,
        orig_trans_pos,
        orig_trans_store,
        parent_item_no_,
        parent_line_no_,
        periodic_disc__group,
        periodic_disc__type,
        periodic_discount,
        posting_exception_key,
        price,
        price_change,
        price_group_code,
        price_in_barcode,
        promotion_no_,
        
        receipt_no_,
        recommended_item,
        refund_qty_,
        refunded_line_no_,
        refunded_pos_no_,
        refunded_store_no_,
        refunded_trans__no_,
        replicated,
        replication_counter,
        
        return_no_sale,
        sales_staff,
        sales_tax_rounding,
        sales_type,
        scale_item,
        section,
        serial_lot_no__not_valid,
        serial_no_,
        shelf,
        shift_date,
        shift_no_,
        staff_id,
        standard_net_price,
        statement_code,
        tax_group_code,
        time,
        timestamp,
        tot__disc_info_line_no_,
        total_disc__,
        total_discount,
        total_rounded_amt_,
        trans__date,
        trans__time,
        transaction_code,
        type_of_sale,
        unit_of_measure,
        uom_price,
        uom_quantity,
        variant_code,
        
        vat_bus__posting_group,
        vat_calculation_type,
        vat_code,
        weight_item,
        weight_manually_entered,
        xstatement_no_,
        xtransaction_status,

case 
when retail_product_code = '31024' then 'Add-on'
when retail_product_code = '31010' then 'Bird Groom'
when retail_product_code = '31011' then 'Cat Groom'
when retail_product_code = '31012' then 'Dog Groom'
when retail_product_code = '31113' then 'Mobile Cat'
when retail_product_code = '31114' then 'Mobile Dog'
else retail_product_code
end as retail_product_code_2,



    from source

)

select * from renamed

--where receipt_no_ = '000000CK02000008408'
--where transaction_no_ = 8575

--WHERE retail_product_code IN ('31024','31010','31011','31012','31113','31114')

