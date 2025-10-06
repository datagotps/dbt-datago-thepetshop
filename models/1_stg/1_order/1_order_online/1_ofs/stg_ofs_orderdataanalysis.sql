with 

source as (
    select * from {{ source('mysql_ofs', 'orderdataanalysis') }}
),

renamed as (
    select
        -- Primary Identifiers
        id,
        weborderno,
        referenceorderno,
        customerid,
        
        -- Order Information
        orderdate,
        country,
        ordertype,
        ordercategory,
        
        -- Item Details
        itemid,
        sku,
        
        -- Batch Information
        batchid,
        batchdatetime,
        ispna,
        
        -- Picking Details
        picked,
        pickedbarcode,
        pickbin,
        pickeddatetime,
        pickedby,
        
        -- Allocation Details
        allocated,
        allocateddatetime,
        allocatedby,
        
        -- Packing Details
        boxid,
        boxdatetime,
        packedby,
        
        -- Shipping Information
        awbno,
        dspcode,
        manifestno,
        manifestdate,
        deliverydate,
        
        -- Order Status Flags
        iscancelled,
        ishold,
        isdelivered,
        isshipped,
        isbypassqc,
        isgateentry,
        
        -- Financial Information - Order Amounts
        orderamount,
        invoiceno,
        invoicedate,
        invoiceamount,
        invoiceamount_usd,
        invoiceamount_kwd,
        storecredit,
        storecredit_kwd,
        collectableamount,
        collectableamount_usd,
        collectableamount_kwd,
        
        -- Payment Information
        paymentmethodcode,
        paymentgateway,
        transactionid,
        transctionamount,
        paymentdate,
        ispaymentreceived,
        
        -- DSP Payment Details
        dspcashreceived,
        dsponlinereceived,
        forwardcycleclose,
        
        -- Return Information
        returninitiatedate,
        pnr,
        returntype,
        returndsp,
        returnticket,
        returnawbno,
        returnbeforedelivery,
        returngateentrydate,
        isreturn,
        
        -- Return Financial Details
        refundamount,
        refundpaymentmethod,
        returntransactionid,
        returntransactiondate,
        
        -- Return QC Information
        qcstatus,
        returnqcdate,
        returncycleclose,
        
        -- Agent Information
        agentcode,
        agentcommission,
        
        -- System/Archive Flags
        readyforarchive,
        
        -- Sync Information
        issync,
        syncdatetime,
        syncretrycount,
        syncerrormessage,
        
        -- Fivetran Metadata
        _fivetran_deleted,
        _fivetran_synced

    from source
)

select * from renamed
--where weborderno = 'O30114277S'