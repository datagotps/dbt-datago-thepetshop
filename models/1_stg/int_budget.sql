with

stg_budget as (
    select * from {{ ref('stg_budget') }}
),

unnested_budget as (
    select 
        sb.year_month,
        d as date,  
        sb.daily_budget,
        sb.sales_channel,
        sb.sales_channel_detail,
        sb.budget as monthly_budget_source,
        sb._row,
        sb._fivetran_synced,
        
        -- Current month budget flag
        CASE 
            WHEN EXTRACT(YEAR FROM date(d)) = EXTRACT(YEAR FROM CURRENT_DATE()) 
            AND EXTRACT(MONTH FROM date(d)) = EXTRACT(MONTH FROM CURRENT_DATE()) 
            THEN sb.daily_budget
            ELSE 0
        END AS current_month_budget,
        
        -- Monthly budget (show only once per month/channel/sales_channel_detail combination)
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY DATE_TRUNC(d, MONTH), sb.channel, sb.sales_channel_detail
                ORDER BY d
            ) = 1 
            THEN SUM(sb.daily_budget) OVER (
                PARTITION BY DATE_TRUNC(d, MONTH), sb.channel, sb.sales_channel_detail
            )
            ELSE NULL 
        END AS monthly_budget,
        
        -- Total days in month (show only once per month/channel)
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY DATE_TRUNC(d, MONTH), sb.channel
                ORDER BY d
            ) = 1 
            THEN MAX(
                DATETIME_DIFF(
                    DATE(DATETIME_SUB(DATETIME_ADD(DATETIME_TRUNC(d, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY)),
                    DATE_TRUNC(d, MONTH),
                    DAY
                ) + 1
            ) OVER (
                PARTITION BY DATE_TRUNC(d, MONTH), sb.channel
            )
            ELSE NULL 
        END AS days_total,
        
        -- Day of month (show only once per date/channel)
        CASE 
            WHEN ROW_NUMBER() OVER (
                PARTITION BY d, sb.channel
                ORDER BY sb.sales_channel_detail
            ) = 1 
            THEN EXTRACT(DAY FROM d)
            ELSE NULL
        END as day_of_month,
        
        -- Partition budget (sum all budgets for same date/channel)
        CASE 
            WHEN ROW_NUMBER() OVER (
                PARTITION BY d, sb.channel
                ORDER BY sb.sales_channel_detail
            ) = 1 
            THEN SUM(sb.daily_budget) OVER (
                PARTITION BY d, sb.channel
            ) 
            ELSE NULL
        END as partition_budget

    from stg_budget as sb
    cross join UNNEST(sb.date_range) as d
)

select * from unnested_budget