with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_sales_header_230a02db_21eb_4476_8cfe_fef4ff006531') }}

),

renamed as (

    select
        weborderid,
        document_type,
        no_,
        
        _fivetran_deleted,
        _fivetran_synced,
        applicationid,
        customernotes,
        timestamp,
        webincrementid,
        

    from source

)

select * from renamed


