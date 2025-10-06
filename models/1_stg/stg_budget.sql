with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'tps_budget_target') }}

),

renamed as (

    select
        _row,
        _fivetran_synced,
        date,
        channel,
        case 
        when location = 'Marina' THEN 'MRI' 
        when location = 'UMM' THEN 'UMSQ'
        
        else location end as sales_channel_detail,


        budget,

        -- Channel mapping
        CASE 
            WHEN channel = 'Shop Sales' THEN 'Shop'
            WHEN channel = 'Direct Online Web' THEN 'Online'
            WHEN channel = 'Affiliates' THEN 'Affiliate'
            
            WHEN channel IN ('P&M', 'Pet Relocation', 'Shop Grooming', 'Mobile Grooming') THEN 'Service'
            ELSE channel
        END as sales_channel,


        PARSE_DATE('%Y-%m-%d', CONCAT(FORMAT_TIMESTAMP('%Y-%m', date), '-01')) as year_month,
        budget / CAST(DATETIME_DIFF(DATETIME_ADD(DATETIME_TRUNC(date, MONTH), INTERVAL 1 MONTH), DATETIME_TRUNC(date, MONTH), DAY) AS FLOAT64) AS daily_budget,
        GENERATE_DATE_ARRAY(DATE(DATETIME_TRUNC(date, MONTH)), DATE(DATETIME_SUB(DATETIME_ADD(DATETIME_TRUNC(date, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY))) AS date_range,


    from source

)

select * from renamed
