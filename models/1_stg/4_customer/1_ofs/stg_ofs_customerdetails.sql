with 

source as (

    select * from {{ source('mysql_ofs', 'customerdetails') }} 


),

renamed as (

    select
        id,
        customerid,
        emailid,
        insertedon,

        magentosyncdatetime,
        categorychangedate,


        addresschangecount,
        cancelitemcount,
        cancelordercount,
        cancelvalue,
        
        cireturnitemcount,
        cireturnordercount,
        cireturnvalue,
        compensationitemcount,
        compensationordercount,
        compensationvalue,
        
        customertype,
        deliveredordercount,
        
        exchangeitemcount,
        exchangeordercount,
        exchangevalue,
        insertedby,
        
        issynctomagento,
        itemcount,
        magentoerrormessage,
        
        ndreturnitemcount,
        ndreturnordercount,
        ndreturnvalue,
        ordercount,
        ordermodifycount,
        ordermodifylinecount,
        ordermodifyvalue,
        ordervalue,
        --rank,
        returnitemcount,
        returnordercount,
        returnpercentage,
        returnvalue,
        updatedby,
        updatedon,
        _fivetran_deleted,
        _fivetran_synced,

    from source

)

select * from renamed

where _fivetran_deleted is false

--and  customerid= '6927580364853'

--cand emailid = 'saitcheson@gulfcapital.com'

--and cancelordercount != 0