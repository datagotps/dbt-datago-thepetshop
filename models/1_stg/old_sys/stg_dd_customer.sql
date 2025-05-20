with 

source as (

    select * from {{ source('Archive_Old_Online_Data', 'DD_Customer') }}

),

renamed as (

    select
    e_mail,
    phone_no,
    CASE 
    WHEN e_mail IS NULL THEN 'Missing'
    WHEN REGEXP_CONTAINS(e_mail, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') THEN 'Valid Email'
    ELSE 'Invalid Email'
  END AS email_status,

        --no,
        name,
        name_2,
        address,
        address_2,
        city,
        country_region_code,
        
        int64_field_8,
        bool_field_9,
        

    from source

)

select * from renamed

 --where e_mail = 'thetosneys-insingapore@live.co.uk'