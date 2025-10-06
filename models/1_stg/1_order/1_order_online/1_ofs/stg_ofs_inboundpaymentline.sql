with 

source as (

    select * from {{ source('mysql_ofs', 'inboundpaymentline') }}

),

renamed as (

    select
        -- Primary Identifiers
        id,
        weborderno,
        itemid,
        
        -- Pricing (Base)
        mrpprice,
        unitprice,
        unitpriceincludingtax,
        amount,
        amountincltax,
        
        -- Discounts
        discount,
        discounttype,
        couponamount,
        couponcode,
        invoicediscountamount,
        invoicediscounttax,
        invoicediscountwithtax,
        
        -- Additional Charges
        codcharges,
        shippingcharges,
        customizedcharges,
        giftcharges,
        othercharges,
        
        -- Tax & Customs
        tax,
        taxpercentage,
        customduty,
        customdutypercentage,
        customthresholdlimit,
        
        -- Payment Gateways
        paymentmethodcode,
        paymentgateway,
        paymentgateway2,
        paymentgateway3,
        paymentgateway4,
        paymentgatewayamount,
        paymentgatewayamount2,
        paymentgatewayamount3,
        paymentgatewayamount4,
        
        -- Wallet & Store Credit
        walletamount,
        walletname,
        storecredit,
        
        -- Loyalty
        loyaltypoints,
        loyalitypointamount,
        
        -- Agent/Commission
        agentcode,
        agentcommission,
        
        -- Transaction Details
        transactionid,
        authorizationid,
        
        -- Currency
        currencycode,
        currencyfactor,
        
        -- Calculation & Adjustments
        recalculatedamount,
        roundingdifference,
        isrecalculated,
        
        -- Processing Flags
        isheader,
        orderchargesprocessed,
        celebrityordersync,
        readyforarchive,
        
        -- Error Handling
        errormessage,
        retrycounter,
        
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