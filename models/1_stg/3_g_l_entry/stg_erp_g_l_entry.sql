with 

source_main as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_g_l_entry_437dbf0e_84ff_417a_965d_ed2bb9650972') }}
),

source_additional as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_g_l_entry_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}
),

joined as (
    select
        a.*,
        b.web_order_no_,
        b.category,
        b.categorydescription,
        b.dimension_1_description,
        b.dimension_2_description,
        b.dimension_3_code,
        b.dimension_3_description,
        b.dimension_4_code,
        b.dimension_4_description,
        b.dimension_5_code,
        b.dimension_5_description,
        b.dimension_6_code,
        b.dimension_6_description,
        b.dimension_7_code,
        b.dimension_7_description,
        b.dimension_8_code,
        b.dimension_8_description,
        b.division,
        b.divisiondescription,
        b.docket_no,
        b.g_l_name,
        b.monthname,
        b.order_category,
        b.order_type,
        b.processed,
        b.year
    from source_main as a
    left join source_additional as b 
        on a.entry_no_ = b.entry_no_
),

renamed as (
    select
        entry_no_,
        gen__posting_type,
        source_type,

        document_date,
        posting_date,
        web_order_no_,
        vat_bus__posting_group,
        vat_amount,
        amount,
        gen__bus__posting_group,
        
        document_type,
        gen__prod__posting_group,

        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        add__currency_credit_amount,
        add__currency_debit_amount,
        additional_currency_amount,
        
        bal__account_no_,
        bal__account_type,
        business_unit_code,
        category,
        categorydescription,
        close_income_statement_dim__id,
        credit_amount,
        debit_amount,
        description,
        dimension_1_description,
        dimension_2_description,
        dimension_3_code,
        dimension_3_description,
        dimension_4_code,
        dimension_4_description,
        dimension_5_code,
        dimension_5_description,
        dimension_6_code,
        dimension_6_description,
        dimension_7_code,
        dimension_7_description,
        dimension_8_code,
        dimension_8_description,
        dimension_set_id,
        division,
        divisiondescription,
        docket_no,
        document_no_,
        
        external_document_no_,
        fa_entry_no_,
        fa_entry_type,
        g_l_account_no_,
        g_l_name,

        global_dimension_1_code,
        global_dimension_2_code,
        ic_partner_code,
        job_no_,
        journal_batch_name,
        last_modified_datetime,
        monthname,
        no__series,
        order_category,
        order_type,
        prior_year_entry,
        processed,
        prod__order_no_,
        quantity,
        reason_code,
        reversed,
        reversed_by_entry_no_,
        reversed_entry_no_,
        source_code,
        source_no_,
        
        system_created_entry,
        tax_area_code,
        tax_group_code,
        tax_liable,
        timestamp,
        transaction_no_,
        use_tax,
        user_id,
        
        
        vat_prod__posting_group,
        year

    from joined
)

select * from renamed

--where  web_order_no_ =  'O2664679'


-- Gen. Posting Type: Purchase, Sale, Settlement
-- Source Type: Customer,Vendor,Bank Account,Fixed Asset,JIC Partner,Employee

--Document Type: Payment, Invoice, Credit Memo, Finance Charge Memo, Reminder, Refund