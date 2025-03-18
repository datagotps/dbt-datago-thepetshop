with 


ofs_ordercount  as 
    (

        select
        weborderno,
        b.location,
        max(locationstatus) as locationstatus,
        max(totalitemcount) totalitemcount,
        count(*)


        from {{ ref('stg_ofs_ordercount') }} as a
        left join {{ ref('stg_ofs_locationmaster') }} as b on SAFE_CAST(a.packaginglocation AS INT64) = b.id
        --where a.__hevo__source_modified_at is null  --and   a.weborderno= 'O3076606S'
        group by 1,2

    )
,

ofs_address  as 
    (

        select
        weborderno,
        Concat(Region,' - ', Street) as address,

        from  {{ ref('stg_ofs_inboundorderaddress') }}
        where addressdetailtype = 'Ship'

    )

    
select 

a.weborderno,
c.location,
c.locationstatus,
a.order_date,
c.totalitemcount,
d.address,

from  {{ ref('stg_ofs_inboundsalesheader') }} as a
left join ofs_ordercount as c on a.weborderno = c.weborderno
left join ofs_address as d on d.weborderno = a.weborderno
--WHERE  a.weborderno= 'O3076992S'

order by a.weborderno desc, a.order_date desc 




