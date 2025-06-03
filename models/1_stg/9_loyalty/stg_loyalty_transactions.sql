with 

source as (

    select * from {{ source('loyalty_aws', 'transactions') }}

),

renamed as (

    select
        transactionid,
        memberid,
        channelid,
        memberdata,
        items,
        tenantid,
        header,
        assignedtomemberat,
        matched,
        grossvalue,
        currency,
        pointsearned,
        unitsdeducted,
        channelname

    from source

)

select * from renamed
