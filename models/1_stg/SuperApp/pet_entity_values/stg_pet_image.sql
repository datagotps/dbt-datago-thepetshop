with 

source as (

    select * from {{ source('public', 'PetImage') }}

),

renamed as (

    select
        id,
        image,
        isprimary,
        createdat,
        updatedat,
        deletedat,
        petid,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed