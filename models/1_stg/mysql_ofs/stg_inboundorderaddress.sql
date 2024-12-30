With source as (
 select * from {{ source(var('ofs_source'), 'inboundorderaddress') }}
)
select 
    id,
    weborderno,

    addressblock,
    addressdetailtype, --Ship, Bill


    addressid,
    addresslabel,
    addresstype,
    alternatephoneno,
    avenue,
    city,
    company,
    country,
    customerid,
    emailid,
    firstname,
    flatno,
    floorno,
    insertedby,
    insertedon,
    landline,
    lastname,
    latitude,
    longitude,
    middlename,
    notes,
    orderdate,
    pacino,
    phoneno,
    postcode,
    readyforarchive,
    region,
    state,
    street,
    telephonecode,
    updateaddress,
    updatedby,
    updatedon,
    villa,

    _fivetran_deleted,
    _fivetran_synced,


current_timestamp() as ingestion_timestamp,




from source 