With source as (
 select * from {{ source(var('ofs_source'), 'orderstatusmaster') }}
)
select 
    id,
    statusname,

    insertedby,
    insertedon,
    isactive,
    processsequence,
    readyforarchive,
    
    updatedby,
    updatedon,

    _fivetran_deleted,
    _fivetran_synced,


current_timestamp() as ingestion_timestamp,




from source 


{#
statusname → id

ReadytoShip →  6
Shipped →  7
Delivered →  8 
UnHold →  9 
Hold →  10 
Cancel →  11 
Returninitiated →  12 
Customer Denied →  18 
Re-Shelved →  20 
CRM PENDING CONFIRM →  28 
CRM ASSIGNED →  29 
WAREHOUSE PROCESSED →  30 
PACKED →  31 
PRECONFIRM →  34 
PREHOLD →  35 
Manifested →  41 
Shelved →  44 
Out For Delivery →  46 
HoldByDriver →  47 
PrePack →  52 
ScheduleRequest →  53 
Lost in Transit →  55
Partial Shipped →  56
Partial Delivered →  57
Partial Packed →  58
Scheduled →  59
Return Reject →  60
Driver Accept
Driver Reject →  63
Driver at Customer Door →  64
OutForDelivery →  65
Rescheduled →  66
OLDERP →  9999

#}
