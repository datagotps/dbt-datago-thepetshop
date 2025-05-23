with 

source as (

    select * from {{ source('loyalty_aws', 'members') }}

),

renamed as (

    select
        memberid,
        tenantid,
        active,
        channelid,
        channelidentifier,
        firstname,
        lastname,
        gender,
        email,
        phone,
        birthdate,
        lastlevelrecalculation,
        address,
        loyaltycardnumber,
        createdat,
        updatedat,
        levelid,
        manuallyassignedlevelid,
        agreement1,
        agreement2,
        agreement3,
        company,
        transactionscount,
        transactionsamount,
        transactionsamountwithoutdeliverycosts,
        transactionsamountexcludedforlevel,
        averagetransactionamount,
        lasttransactiondate,
        firsttransactiondate,
        levelachievementdate,
        referrermemberid,
        labels,
        anonymized,
        referraltoken,
        defaultaccount,
        currency,
        storecode,
        currentlevel,
        registeredat

    from source

)

select * from renamed
