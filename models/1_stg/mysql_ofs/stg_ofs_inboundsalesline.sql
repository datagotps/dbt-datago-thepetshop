with 

source as (

    select * from {{ source('mysql_ofs', 'inboundsalesline') }}

),

renamed as (

    select
        itemid,
        weborderno,
        websitelineno,
        couponcode,
        campaignid,
        productid,
        description,
        sku,
        quantity,
        bundleid,
        bundlequantity,
        expecteddispatchdate,
        itemno,
        vendorid,
        deliverytype,
        specialdeliverydate,
        isfragile,
        isprecious,
        issurface,
        iscustomized,
        picklocation,
        packaginglocation,
        dspcode,
        isprocessed,
        readyforarchive,
        insertedon,
        insertedby,
        updatedon,
        updatedby,
        leafcategory,
        isfoc,
        isdanger,
        arabicdescription,
        bundleseqid,
        bundledescription,
        rowid,
        bundleproductid,
        isgwp,
        offertype,
        inventorytype,
        itemtype,
        ispickable,
        olditemno,
        shopifyfulfilmentorderid,
        shopifyfulfilmentorderlineid,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
