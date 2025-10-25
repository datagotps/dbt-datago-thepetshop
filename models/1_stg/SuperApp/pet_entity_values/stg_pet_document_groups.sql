with 

source as (

    select * from {{ source('public', 'PetDocumentGroup') }}

),

renamed as (

    select
        id,
        petid,
        type,
        name,
        notes,
        issuedate,
        expirydate,
        createdat,
        updatedat,
        deletedat,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed