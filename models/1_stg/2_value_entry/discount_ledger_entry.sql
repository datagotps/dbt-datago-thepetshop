with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_discount_ledger_entry_5ecfc871_5d82_43f1_9c54_59685e82318d') }}

),

renamed as (

    select
        entry_no_,
        offer_no_,
        offer_type,
        sales_amount,
        _systemid,
        item_ledger_entry_no_,
        discount_amount,
        quantity,
        entry_type,
        posting_date,
        quantity_factor,
        sales_amount_factor,
        cost_amount,
        discount_factor,
        document_no_,
        timestamp,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
