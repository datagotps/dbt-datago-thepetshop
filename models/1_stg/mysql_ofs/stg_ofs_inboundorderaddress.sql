With source as (
 select * from {{ source(var('ofs_source'), 'inboundorderaddress') }}
)
select 
    id,
    weborderno,

    orderdate,

--order address
    addressdetailtype, --Ship, Bill
    state,
    region,
    city,
    
    street,
    latitude,
    longitude,
    flatno,
    floorno,
    villa,

    notes,



--Customer
    customerid,
    firstname,
    middlename,
    lastname,
    emailid,
    landline,
    phoneno,



    addressblock,
    


    addressid,
    addresslabel,
    addresstype,
    alternatephoneno,
    avenue,
    company,
    country,


    insertedby,
    insertedon,
    
    
    
    pacino,
    
    postcode,
    readyforarchive,
    
    telephonecode,
    updateaddress,
    updatedby,
    updatedon,

    _fivetran_deleted,
    _fivetran_synced,


current_timestamp() as ingestion_timestamp,




from source 