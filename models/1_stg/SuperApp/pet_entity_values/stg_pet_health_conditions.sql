with 

source as (

    select * from {{ source('public', 'PetHealthConditions') }}

),

renamed as (

    select
        id,
        createdat,
        updatedat,
        petid,
        healthconditionid,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed