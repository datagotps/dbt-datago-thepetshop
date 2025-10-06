with 

source as (

    select * from {{ source('mysql_ofs', 'inboundsalesline') }}

),

renamed as (

    select
        -- Primary Identifiers
        rowid,
        itemid,
        weborderno,
        websitelineno,
        
        -- Product Information
        productid,
        itemno,
        olditemno,
        sku,
        description,
        arabicdescription,
        leafcategory,
        
        -- Bundle Information
        bundleid,
        bundleseqid,
        bundleproductid,
        bundledescription,
        bundlequantity,
        
        -- Quantities
        quantity,
        
        -- Vendor & Inventory
        vendorid,
        inventorytype,
        itemtype,
        
        -- Promotions & Offers
        couponcode,
        campaignid,
        offertype,
        isgwp,
        isfoc,
        
        -- Delivery & Location
        deliverytype,
        picklocation,
        packaginglocation,
        dspcode,
        
        -- Delivery Dates
        expecteddispatchdate,
        specialdeliverydate,
        
        -- Product Attributes/Flags
        isfragile,
        isprecious,
        issurface,
        iscustomized,
        isdanger,
        ispickable,
        
        -- Processing Status
        isprocessed,
        readyforarchive,
        
        -- Shopify Integration
        shopifyfulfilmentorderid,
        shopifyfulfilmentorderlineid,
        
        -- Audit Fields
        insertedon,
        insertedby,
        updatedon,
        updatedby,
        
        -- System/Metadata Fields
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed