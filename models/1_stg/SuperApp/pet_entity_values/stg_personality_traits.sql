with 

source as (

    select * from {{ source('public', 'PersonalityTrait') }}

),

renamed as (

    select
        id,
        value,
        label,
        icon,
        imageurl,
        isactive,
        `order` as sort_order,
        createdat,
        updatedat,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed