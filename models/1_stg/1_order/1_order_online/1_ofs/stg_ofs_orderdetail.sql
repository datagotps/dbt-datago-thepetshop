With source as (
 select * from {{ source(var('ofs_source'), 'orderdetail') }}
)
select 
    id,
    weborderno,

    itemid,
    itemno,


    batchcreated,
    batchid,
    batchtype,
    country,
    deliverytype,
    dspcode,
    insertedby,
    insertedon,
    inventorytype,
    iscancelled,
    iscustomized,
    isfoc,
    isgifting,
    ishold,
    issurface,
    
    
    ordercategory,
    ordertype,
    packaginglocation,
    picklocation,
    promiseddeliverydate,
    readyforarchive,
    referenceorderno,
    repickcount,
    tableid,
    updatedby,
    updatedon,
    vendorid,
    
    _fivetran_deleted,
    _fivetran_synced,
   
    


current_timestamp() as ingestion_timestamp,




from source 

where _fivetran_deleted is false

--and weborderno= 'O30114277S'