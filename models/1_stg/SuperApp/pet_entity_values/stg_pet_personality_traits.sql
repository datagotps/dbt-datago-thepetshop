with 

source as (

    select * from {{ source('public', 'PetPersonalityTraits') }}

),

renamed as (

    select
        id,
        createdat,
        updatedat,
        petid,
        personalitytraitid,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed