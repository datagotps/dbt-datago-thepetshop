with 

source as (

    select * from {{ source('public', 'users') }}

),

renamed as (

    select
        id,
        createdat,
        updatedat,
        deletedat,
        firstname,
        lastname,
        email,
        country,
        internationalcode,
        phone,
        isguest,
        isactive,
        isnotificationsenabled,
        isemailverified,
        isphoneverified,
        privacypolicy,
        termsandconditions,
        created_at,
        updated_at,
        gender,
        birthdate,
        nationality,
        preferredlanguage,
        isprofilecomplete,
        profilecompletedat,
        shopify_id,
        moengageid,
        _fivetran_deleted,
        _fivetran_synced,
        openloyaltyprofilerewarded,
        openloyaltymemberid,
        moegoid,
        openloyaltyfirstpetrewarded

    from source

)

select * from renamed