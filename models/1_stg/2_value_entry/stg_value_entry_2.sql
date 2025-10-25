with 

source as (

    select 
    a.*,
    b.description as offline_offer_name,

    from {{ source('sql_erp_prod_dbo', 'petshop_value_entry_5ecfc871_5d82_43f1_9c54_59685e82318d') }} as a 
    left join  {{ ref('stg_periodic_discount') }}  as b on a.offer_no_ = b.no_

),

renamed as (

    select

        offline_offer_name,
        offer_no_,


        item_category,
        retail_product_code,
        division,
        promotion_no_,
        
        batch_no_,
        vendor_no_,
        inv__adjust__group,


        entry_no_,
        lsc_variant_code,
        lsc_discount_amount,
        lsc_cost_amount__actual_,
        lsc_item_ledger_entry_type,
        lsc_global_dimension_1_code,
        
        
        lsc_posting_date,
        
        lsc_location_code,
        lsc_item_no_,
        bi_timestamp,
        lsc_global_dimension_2_code,
        lsc_valued_quantity,
        lsc_sales_amount__actual_,
        
        
        
        
        lsc_invoiced_quantity,
        lsc_salespers__purch__code,
        
        timestamp,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed

--where entry_no_ = 22954217

