with 

source as (

    select * from {{ source('public', 'PetSubType') }}

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
        isother,
        createdat,
        updatedat,
        typeid,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed