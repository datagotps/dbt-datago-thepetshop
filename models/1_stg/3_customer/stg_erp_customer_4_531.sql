with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_customer_230a02db_21eb_4476_8cfe_fef4ff006531') }}

),

renamed as (

    select
        no_,
        webid,
        applicationid,

        _fivetran_deleted,
        _fivetran_synced,
        
        timestamp,
        

    from source

)

select * from renamed

--where no_ = 'C000066928'
