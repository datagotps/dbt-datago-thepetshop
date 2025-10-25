with 

source as (

    select * from {{ source('public', 'PetAllergies') }}

),

renamed as (

    select
        id,
        createdat,
        updatedat,
        petid,
        allergyid,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed