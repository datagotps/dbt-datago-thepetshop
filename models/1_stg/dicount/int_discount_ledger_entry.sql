
with source as (
    select
    *
    from {{ ref('stg_discount_ledger_entry') }} 
),

aggregated as (
    select
        item_ledger_entry_no_,
        
        -- Aggregate multiple discounts
        sum(discount_amount) as total_discount_amount,
        sum(sales_amount) as total_sales_amount,
        sum(quantity) as total_quantity,
        sum(cost_amount) as total_cost_amount,
        
        -- Concatenate offer names if multiple (BigQuery syntax)
        string_agg(offline_offer_name, ', ' order by entry_no_) as combined_offer_names,
        
        -- Keep other fields (taking first occurrence)
        max(offline_offer_name) as offline_offer_name,
        max(offer_no_) as offer_no_,
        max(offer_type) as offer_type,
        max(entry_type) as entry_type,
        max(posting_date) as posting_date,
        max(document_no_) as document_no_,
        max(timestamp) as timestamp,
        max(_fivetran_synced) as _fivetran_synced,

        
        count(*) as discount_count,  -- Track how many discounts were combined
        case 
            when count(*) > 1 then true 
            else false 
        end as has_multiple_offers

    from source
    where _fivetran_deleted = false  -- Exclude deleted records
    group by item_ledger_entry_no_
)

select * from aggregated

--where item_ledger_entry_no_ = 14047488
