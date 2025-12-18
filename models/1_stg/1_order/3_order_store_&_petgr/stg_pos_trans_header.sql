
--this table include the POS retal sales transaction.
--CONCAT(store_no_, '-', pos_terminal_no_, '-', transaction_no_) AS document_no_

--where CONCAT(store_no_, '-', pos_terminal_no_, '-', transaction_no_) = 'CRK-CK01-11691'


with 
source as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_transaction_header_5ecfc871_5d82_43f1_9c54_59685e82318d') }}
),

renamed as (
    select
        -- PRIMARY IDENTIFIERS
        CONCAT(store_no_, '-', pos_terminal_no_, '-', transaction_no_) AS document_no_,
        store_no_,
        pos_terminal_no_,
        transaction_no_,
        receipt_no_,
        refund_receipt_no_,
        retrieved_from_receipt_no_,
        
        -- TRANSACTION TYPE & STATUS
        transaction_type,
        transaction_code,
        sales_type,
        entry_status,
        sale_is_return_sale,
        trans__is_mixed_sale_refund,
        customer_order,
        customer_order_id,
        post_as_shipment,
        
        -- DATE & TIME FIELDS
        date AS pos_posting_date,              -- When transaction was finalized/posted
        original_date,
        time AS pos_posting_time,              -- Time when finalized
        time_when_total_pressed,
        time_when_trans__closed,
        shift_date,
        shift_no_,
        wrong_shift,
        
        -- CUSTOMER INFORMATION
        customer_no_,
        member_card_no_,
        sell_to_contact_no_,
        customer_disc__group,
        tax_exemption_no_,
        tax_liable,
        tax_area_code,
        vat_bus_posting_group,
        
        -- STAFF & TERMINAL INFO
        staff_id,
        manager_id,
        created_on_pos_terminal,
        
        -- AMOUNT FIELDS
        gross_amount,
        net_amount,
        payment,
        cost_amount,
        discount_amount,
        customer_discount,
        total_discount,
        income_exp__amount,
        net_income_exp__amount,
        amount_to_account,
        trans__sale_pmt__diff_,
        reverted_gross_amount,
        rounded,
        trans__currency,
        
        -- LINE & ITEM COUNTS
        no__of_items,
        no__of_item_lines,
        no__of_payment_lines,
        no__of_invoices,
        no__of_covers,
        
        -- SPECIAL FEATURES & CODES
        gift_registration_no_,
        infocode_disc__group,
        contains_forecourt_items,
        wic_transaction,
        open_drawer,
        comment,
        
        -- RESTAURANT/HOSPITALITY SPECIFIC
        table_no_,
        split_number,
        
        -- ACCOUNTING & POSTING
        apply_to_doc__no_,
        to_account,
        statement_code,
        statement_no____not_used,
        source_type,
        items_posted,
        
        -- SAFE & CASH MANAGEMENT
        safe_code,
        safe_entry_no_,
        starting_point_balance,
        
        -- REPORTING
        y_report_id,
        z_report_id,
        counter,
        
        -- REPLICATION & SYNC
        replicated,
        replication_counter,
        playback_entry_no_,
        playback_recording_id,
        
        -- SYSTEM FIELDS
        _systemid,
        timestamp,
        bi_timestamp,
        _fivetran_synced,
        _fivetran_deleted

    from source
)

select * from renamed
--where CONCAT(store_no_, '-', pos_terminal_no_, '-', transaction_no_) = 'DIP-DT01-132998'

--where CONCAT(store_no_, '-', pos_terminal_no_, '-', transaction_no_) = 'CRK-CK01-489'
--where no__of_items >1
--where transaction_no_ = 8575
--where receipt_no_ = '000000CK02000008408'
--where document_no_ = 'CRK-CK02-8575'