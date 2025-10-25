with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_periodic_discount_5ecfc871_5d82_43f1_9c54_59685e82318d') }}

),

renamed as (

    select
    no_, --offer_no_
    discount_type,
    description, --offer_name
     
    from source

)

select * from renamed

