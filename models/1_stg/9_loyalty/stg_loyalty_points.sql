with 

source as (

    select * from {{ source('loyalty_aws', 'points') }}

),

renamed as (

    select
        transferid,
        tenantid,
        points,
        memberid,
        walletid,
        wallettypecode,
        firstname,
        lastname,
        email,
        phone,
        loyaltycardnumber,
        type,
        createdat,
        expiredat,
        cancelled,
        cancelledat,
        locked,
        unlockat,
        comment,
        campaignid,
        transactionid,
        customeventid,
        internaleventname

    from source

)

select * from renamed
