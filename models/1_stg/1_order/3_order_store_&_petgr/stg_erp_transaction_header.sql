
--this table include the POS retal sales transaction.
--CONCAT(store_no_, '-', pos_terminal_no_, '-', transaction_no_) AS document_no_

--where CONCAT(store_no_, '-', pos_terminal_no_, '-', transaction_no_) = 'CRK-CK01-11691'


with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_transaction_header_5ecfc871_5d82_43f1_9c54_59685e82318d') }}

),

renamed as (

    select
        CONCAT(store_no_, '-', pos_terminal_no_, '-', transaction_no_) AS document_no_,
        receipt_no_,
        
        pos_terminal_no_,
        store_no_,
        transaction_no_,

        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        amount_to_account,
        apply_to_doc__no_,
        bi_timestamp,
        comment,
        contains_forecourt_items,
        cost_amount,
        counter,
        created_on_pos_terminal,
        customer_disc__group,
        customer_discount,
        customer_no_,
        customer_order,
        customer_order_id,
        date,
        discount_amount,
        entry_status,
        gift_registration_no_,
        gross_amount,
        income_exp__amount,
        infocode_disc__group,
        items_posted,
        manager_id,
        member_card_no_,
        net_amount,
        net_income_exp__amount,
        no__of_covers,
        no__of_invoices,
        no__of_item_lines,
        no__of_items,
        no__of_payment_lines,
        open_drawer,
        original_date,
        payment,
        playback_entry_no_,
        playback_recording_id,
        post_as_shipment,
        
        refund_receipt_no_,
        replicated,
        replication_counter,
        retrieved_from_receipt_no_,
        reverted_gross_amount,
        rounded,
        safe_code,
        safe_entry_no_,
        sale_is_return_sale,
        sales_type,
        sell_to_contact_no_,
        shift_date,
        shift_no_,
        source_type,
        split_number,
        staff_id,
        starting_point_balance,
        statement_code,
        statement_no____not_used,
        table_no_,
        tax_area_code,
        tax_exemption_no_,
        tax_liable,
        time,
        time_when_total_pressed,
        time_when_trans__closed,
        timestamp,
        to_account,
        total_discount,
        trans__currency,
        trans__is_mixed_sale_refund,
        trans__sale_pmt__diff_,
        transaction_code,
        transaction_type,
        vat_bus_posting_group,
        wic_transaction,
        wrong_shift,
        y_report_id,
        z_report_id

    from source

)

select * from renamed

--where transaction_no_ = 8575

--where receipt_no_ = '000000CK02000008408'
--where document_no_ = 'CRK-CK02-8575'
