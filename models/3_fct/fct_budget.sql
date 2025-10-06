with

int_budget as (
    select * from {{ ref('int_budget') }}
),

final as (
    select 
        b.date,
        b.sales_channel,
        b.sales_channel_detail,
        b.year_month,
        b.monthly_budget,
        b.daily_budget,
        b.current_month_budget,
        b.days_total,
        b.day_of_month,
        b.partition_budget,
        b._row,
        b._fivetran_synced,
        
        -- Budget link (composite key for joins)
        CONCAT(
            COALESCE(b.sales_channel, ''), 
            COALESCE(b.sales_channel_detail, '')
        ) as budget_link,
        
        -- MTD Budget (Month-To-Date)
        CASE 
            WHEN DATE_DIFF(DATE(b.date), CURRENT_DATE(), MONTH) = 0 
            AND EXTRACT(DAY FROM b.date) <= EXTRACT(DAY FROM CURRENT_DATE()) 
            THEN b.daily_budget 
            ELSE 0 
        END as mtd_budget,
        
        -- LMTD Budget (Last Month-To-Date)
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(b.date), MONTH) = 1 
            AND EXTRACT(DAY FROM b.date) <= EXTRACT(DAY FROM CURRENT_DATE()) 
            THEN b.daily_budget 
            ELSE 0 
        END as lmtd_budget,
        
        -- YTD Budget (Year-To-Date)
        CASE 
            WHEN EXTRACT(YEAR FROM b.date) = EXTRACT(YEAR FROM CURRENT_DATE()) 
            AND EXTRACT(MONTH FROM b.date) <= EXTRACT(MONTH FROM CURRENT_DATE())
            AND DATE(b.date) <= CURRENT_DATE() 
            THEN b.daily_budget 
            ELSE 0 
        END AS ytd_budget,
        
        -- Additional helper flags
        CASE 
            WHEN DATE(b.date) = CURRENT_DATE() 
            THEN b.daily_budget 
            ELSE 0 
        END as today_budget,
        
        CASE 
            WHEN DATE(b.date) < CURRENT_DATE() 
            THEN b.daily_budget 
            ELSE 0 
        END as past_budget,
        
        CASE 
            WHEN DATE(b.date) > CURRENT_DATE() 
            THEN b.daily_budget 
            ELSE 0 
        END as future_budget

    from int_budget as b   
)

select * from final