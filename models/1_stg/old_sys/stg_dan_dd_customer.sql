with 

source as (

    select * from {{ source('Archive_Old_Online_Data', 'Dan_DD_Customer') }}

),

renamed as (

    select
        
        contactdetails_phone,
        contactdetails_email,
        createddate,

          CASE 
    WHEN contactDetails_email IS NULL THEN 'Missing'
    WHEN REGEXP_CONTAINS(contactDetails_email, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') THEN 'Valid Email'
    ELSE 'Invalid Email'
  END AS email_status,

        id,

        
        lastmodifieddate,
        companyname,
        addressinformation_address,
        addressinformation_name,
        addressinformation_address2,
        addressinformation_city,
        addressinformation_state,
        addressinformation_zipcode,
        addressinformation_country_id,
        reservedfields_reservedfield1,
        reservedfields_reservedfield2,
        reservedfields_reservedfield3,
        reservedfields_reservedfield4,
        reservedfields_reservedfield5,
        customertypeenum,
        
        

    from source

)

select * from renamed

 --where contactdetails_email = 'thetosneys-insingapore@live.co.uk'
 --where contactdetails_email is null