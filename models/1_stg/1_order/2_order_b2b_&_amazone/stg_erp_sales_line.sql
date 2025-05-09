with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_sales_line_230a02db_21eb_4476_8cfe_fef4ff006531') }}

),

renamed as (

    select
        document_no_,
        document_type,
        line_no_,
        _fivetran_deleted,
        _fivetran_synced,
        timestamp,
        webid,
        weblineflag

    from source

)

select * from renamed

--where document_no_ like  'INV%'

--where document_no_ = 'SO/2025/00689'

--where document_type = 5


--document_type
--0: SQ/2024/00069
--1: SO/2025/01068
--2: SI/2024/00847
--3: SCM/2024/00070
--4: null
--5: RSO/2024/00244


