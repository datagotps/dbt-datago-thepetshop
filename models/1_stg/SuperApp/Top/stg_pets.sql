with 

source as (

    select * from {{ source('public', 'Pets') }}

),

renamed as (

    select
        id,
        name,
        weight,
        microchip,
        age,
        size,
        isvaccinated,
        isneutered,
        notes,
        userid,
        isprofilecomplete,
        profilecompletedat,
        birthdate,
        customsubtype,
        customdietarypreferences,
        customallergies,
        deletedat,
        typeid,
        subtypeid,
        activitylevelid,
        createdat,
        updatedat,
        gender,
        hasmicrochip,
        customhealthconditions,
        _fivetran_deleted,
        _fivetran_synced,
        moegoid

    from source

)

select * from renamed