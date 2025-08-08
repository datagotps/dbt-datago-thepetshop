with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_sales_price_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
        currency_code,
        item_no_,
        minimum_quantity,
        sales_code,
        sales_type,
        starting_date,
        unit_of_measure_code,
        variant_code,
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        allow_invoice_disc_,
        allow_line_disc_,
        ending_date,
        price_includes_vat,
        timestamp,
        unit_price,
        vat_bus__posting_gr___price_

    from source

)

select * from renamed
WHERE sales_code = 'AED' AND item_no_ LIKE '%-%'
