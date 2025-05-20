with 

source as (

    select * from {{ source('mysql_ofs', 'inboundpaymentline') }}

),

renamed as (

    select
        id,
        weborderno,
        mrpprice,
        amount,
        amountincltax,
        invoicediscountamount,
        
        isheader,
        itemid,

        loyalitypointamount,
        
        
        agentcode,
        agentcommission,
        
        

        authorizationid,
        celebrityordersync,
        codcharges,
        couponamount,
        couponcode,
        currencycode,
        currencyfactor,
        customduty,
        customdutypercentage,
        customizedcharges,
        customthresholdlimit,
        discount,
        discounttype,
        errormessage,
        giftcharges,
        insertedby,
        insertedon,
        
        invoicediscounttax,
        invoicediscountwithtax,
        
        isrecalculated,
        
        
        loyaltypoints,
        
        orderchargesprocessed,
        othercharges,
        paymentgateway,
        paymentgateway2,
        paymentgateway3,
        paymentgateway4,
        paymentgatewayamount,
        paymentgatewayamount2,
        paymentgatewayamount3,
        paymentgatewayamount4,
        paymentmethodcode,
        readyforarchive,
        recalculatedamount,
        retrycounter,
        roundingdifference,
        shippingcharges,
        storecredit,
        tax,
        taxpercentage,
        transactionid,
        unitprice,
        unitpriceincludingtax,
        updatedby,
        updatedon,
        walletamount,
        walletname,
        
        _fivetran_deleted,
        _fivetran_synced,

    from source

)

select * from renamed
