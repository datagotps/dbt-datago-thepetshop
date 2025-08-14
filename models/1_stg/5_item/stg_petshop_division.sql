with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_division_5ecfc871_5d82_43f1_9c54_59685e82318d') }}

),

renamed as (

    select
        code,
        description,
        profit_goal__,
        buyer_group_code,
        replen__data_profile,
        phys_invt_counting_period_code,
        _systemid,
        timestamp,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
