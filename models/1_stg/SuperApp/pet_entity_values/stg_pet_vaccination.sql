with 

source as (

    select * from {{ source('public', 'PetVaccination') }}

),

renamed as (

    select
        id,
        vaccinename,
        vaccinatedon,
        nextvaccinedate,
        notes,
        createdat,
        updatedat,
        deletedat,
        petid,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed