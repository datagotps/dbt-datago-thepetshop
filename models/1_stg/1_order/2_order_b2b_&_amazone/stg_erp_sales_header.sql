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

--where weborderid != ''

--where no_ = 'SI/2024/00956'


--document_type
--0: SQ/2024/00069.  --Sales Quote
--1: SO/2025/01068.  --Sales Order
--2: SI/2024/00847.  --Sales Invoice
--3: SCM/2024/00070. -- Sales Credit Memos
--4: null
--5: RSO/2024/00244. -- Return Sales Order
