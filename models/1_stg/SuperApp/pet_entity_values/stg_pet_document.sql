with 

source as (

    select * from {{ source('public', 'PetDocument') }}

),

renamed as (

    select
        id,
        document,
        name,
        type,
        issuedate,
        expirydate,
        notes,
        createdat,
        updatedat,
        deletedat,
        petid,
        vaccinationid,
        groupid,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed