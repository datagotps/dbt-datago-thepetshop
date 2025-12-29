With source as (
 select * from {{ ref('int_occ_order_items') }}
)
select 

-- Order Identifiers
weborderno,                        -- dim (web order number)
itemid,                            -- dim (line item ID)
itemno,                            -- dim (item code)
sku,                               -- dim (SKU code)
quantity,                          -- fact (quantity ordered)

-- Product Details
item_name,                         -- dim (product description)
item_division,                     -- dim: Level 1 - Pet (DOG, CAT, FISH, etc.)
item_block,                        -- dim: Level 2 - Block (FOOD, ACCESSORIES, etc.)
item_category,                     -- dim: Level 3 - Category (Dry Food, Wet Food, etc.)
item_brand,                        -- dim: Level 5 - Brand

-- Order Details
orderplatform,                     -- dim: website, Android, iOS, etc
ordersource,                       -- dim: Website, CRM, iOS
customerid,                        -- dim (customer ID)
location,                          -- dim (packaging location name)
ordertype,                         -- dim: EXPRESS, NORMAL, EXCHANGE
paymentmethodcode,                 -- dim: PREPAID, COD
crm_order_line_status,             -- dim: CLOSE, OPEN, etc

-- Revenue Data
gross_subtotal,                    -- fact (AED gross with tax)
gross_subtotal_exclu_tax,          -- fact (AED gross without tax)
net_subtotal,                      -- fact (AED net with tax)
net_subtotal_exclu_tax,            -- fact (AED net without tax)
discount,                          -- fact (AED discount with tax)
discount_exclu_tax,                -- fact (AED discount without tax)

-- Fulfillment Flags
iscancelled,                       -- dim: true, false
isdelivered,                       -- dim: true, false
returned,                          -- dim: 0, 1
batchcreated,                      -- dim: 0, 1
picked,                            -- dim: 0, 1
packed,                            -- dim: 0, 1
awbno,                             -- dim (AWB tracking number)

-- Fulfillment Dates
insertedon,                        -- dim (datetime - to be removed)
order_date,                        -- dim (datetime - to be removed)
shopify_order_datetime,            -- dim (original order datetime)
ofs_order_datetime,                -- dim (OFS inserted datetime)
ofs_order_date,                    -- dim (OFS inserted date)
batchdatetime,                     -- dim (batch created datetime)
pickeddatetime,                    -- dim (picked datetime)
packdatetime,                      -- dim (packed datetime)
deliverydate,                      -- dim (delivery date)

-- Order Sync Metrics
order_sync_minutes,                -- fact (minutes between order and OFS)
order_sync_category,               -- dim: < 10 mins, 10-60 mins, 1-10 hours, 10-24 hours, > 24 hours
order_sync_category_sort,          -- dim: 1-5 (sort order)

-- Revenue Classifications
unfulfilled_revenue,               -- fact (AED not yet fulfilled)
recognized_revenue,                -- fact (AED recognized in ERP)
rec_rev_in_period,                 -- fact (AED recognized in period)
rec_rev_deferred,                  -- fact (AED deferred revenue)
revenue_classification,            -- dim (revenue classification type)
erp_posting_status,                -- dim: Posted, Not Posted
posting_date,                      -- dim (ERP posting date - when invoice was posted)
unfulfilled_sales,                 -- fact (AED difference OFS vs ERP)

-- Shipping
shipping,                          -- fact (AED shipping - first item only)

-- Delivery Mode
delivery_mode,                     -- dim: 60-min Express, 4-Hour Express, Regular

-- On-Time Delivery (OTD) Metrics
customer_emirate,                  -- dim: Dubai, Abu Dhabi, Sharjah, Ajman, etc.

-- Delivery SLA in minutes (based on delivery mode + location)
CASE 
    WHEN delivery_mode = '60-min Express' THEN 60
    WHEN delivery_mode = '4-Hour Express' THEN 240
    WHEN customer_emirate = 'Dubai' 
         AND EXTRACT(HOUR FROM shopify_order_datetime) < 9 THEN 720  -- Same day by 9 PM (12 hrs)
    WHEN customer_emirate = 'Dubai' THEN 1440  -- Next day (24 hrs)
    WHEN customer_emirate IN ('Abu Dhabi', 'Sharjah', 'Ajman', 'Al Ain', 'Ras Al-Khaimah') THEN 1440  -- Next day
    ELSE 4320  -- Remote areas: 3 days (72 hrs)
END AS delivery_sla_minutes,       -- fact: SLA in minutes

-- Actual delivery time in minutes (from order to delivery)
CASE 
    WHEN isdelivered = TRUE AND deliverydate IS NOT NULL AND shopify_order_datetime IS NOT NULL
    THEN DATETIME_DIFF(deliverydate, shopify_order_datetime, MINUTE)
    ELSE NULL
END AS delivery_time_minutes,      -- fact: actual time from order to delivery

-- On-Time Flag (1 = on-time, 0 = late)
CASE 
    WHEN isdelivered = TRUE 
         AND deliverydate IS NOT NULL 
         AND shopify_order_datetime IS NOT NULL
         AND DATETIME_DIFF(deliverydate, shopify_order_datetime, MINUTE) <= 
             CASE 
                 WHEN delivery_mode = '60-min Express' THEN 60
                 WHEN delivery_mode = '4-Hour Express' THEN 240
                 WHEN customer_emirate = 'Dubai' AND EXTRACT(HOUR FROM shopify_order_datetime) < 9 THEN 720
                 WHEN customer_emirate = 'Dubai' THEN 1440
                 WHEN customer_emirate IN ('Abu Dhabi', 'Sharjah', 'Ajman', 'Al Ain', 'Ras Al-Khaimah') THEN 1440
                 ELSE 4320
             END
    THEN 1 
    ELSE 0 
END AS is_on_time,                 -- fact: 1 = on-time, 0 = late

-- Delivery variance in minutes (negative = early, positive = late)
CASE 
    WHEN isdelivered = TRUE AND deliverydate IS NOT NULL AND shopify_order_datetime IS NOT NULL
    THEN DATETIME_DIFF(deliverydate, shopify_order_datetime, MINUTE) - 
         CASE 
             WHEN delivery_mode = '60-min Express' THEN 60
             WHEN delivery_mode = '4-Hour Express' THEN 240
             WHEN customer_emirate = 'Dubai' AND EXTRACT(HOUR FROM shopify_order_datetime) < 9 THEN 720
             WHEN customer_emirate = 'Dubai' THEN 1440
             WHEN customer_emirate IN ('Abu Dhabi', 'Sharjah', 'Ajman', 'Al Ain', 'Ras Al-Khaimah') THEN 1440
             ELSE 4320
         END
    ELSE NULL
END AS delivery_variance_minutes,  -- fact: negative = early, positive = late

-- OTD Performance Category
CASE 
    WHEN isdelivered = FALSE OR isdelivered IS NULL THEN 'Not Delivered'
    WHEN deliverydate IS NULL OR shopify_order_datetime IS NULL THEN 'Missing Data'
    WHEN DATETIME_DIFF(deliverydate, shopify_order_datetime, MINUTE) - 
         CASE 
             WHEN delivery_mode = '60-min Express' THEN 60
             WHEN delivery_mode = '4-Hour Express' THEN 240
             WHEN customer_emirate = 'Dubai' AND EXTRACT(HOUR FROM shopify_order_datetime) < 9 THEN 720
             WHEN customer_emirate = 'Dubai' THEN 1440
             WHEN customer_emirate IN ('Abu Dhabi', 'Sharjah', 'Ajman', 'Al Ain', 'Ras Al-Khaimah') THEN 1440
             ELSE 4320
         END <= -60 THEN 'Early (1hr+)'
    WHEN DATETIME_DIFF(deliverydate, shopify_order_datetime, MINUTE) - 
         CASE 
             WHEN delivery_mode = '60-min Express' THEN 60
             WHEN delivery_mode = '4-Hour Express' THEN 240
             WHEN customer_emirate = 'Dubai' AND EXTRACT(HOUR FROM shopify_order_datetime) < 9 THEN 720
             WHEN customer_emirate = 'Dubai' THEN 1440
             WHEN customer_emirate IN ('Abu Dhabi', 'Sharjah', 'Ajman', 'Al Ain', 'Ras Al-Khaimah') THEN 1440
             ELSE 4320
         END <= 0 THEN 'On-Time'
    WHEN DATETIME_DIFF(deliverydate, shopify_order_datetime, MINUTE) - 
         CASE 
             WHEN delivery_mode = '60-min Express' THEN 60
             WHEN delivery_mode = '4-Hour Express' THEN 240
             WHEN customer_emirate = 'Dubai' AND EXTRACT(HOUR FROM shopify_order_datetime) < 9 THEN 720
             WHEN customer_emirate = 'Dubai' THEN 1440
             WHEN customer_emirate IN ('Abu Dhabi', 'Sharjah', 'Ajman', 'Al Ain', 'Ras Al-Khaimah') THEN 1440
             ELSE 4320
         END <= 60 THEN 'Late (< 1hr)'
    WHEN DATETIME_DIFF(deliverydate, shopify_order_datetime, MINUTE) - 
         CASE 
             WHEN delivery_mode = '60-min Express' THEN 60
             WHEN delivery_mode = '4-Hour Express' THEN 240
             WHEN customer_emirate = 'Dubai' AND EXTRACT(HOUR FROM shopify_order_datetime) < 9 THEN 720
             WHEN customer_emirate = 'Dubai' THEN 1440
             WHEN customer_emirate IN ('Abu Dhabi', 'Sharjah', 'Ajman', 'Al Ain', 'Ras Al-Khaimah') THEN 1440
             ELSE 4320
         END <= 120 THEN 'Late (1-2 hrs)'
    ELSE 'Very Late (2+ hrs)'
END AS otd_category,               -- dim: Early, On-Time, Late categories

-- OTD Category Sort Order
CASE 
    WHEN isdelivered = FALSE OR isdelivered IS NULL THEN 6
    WHEN deliverydate IS NULL OR shopify_order_datetime IS NULL THEN 7
    WHEN DATETIME_DIFF(deliverydate, shopify_order_datetime, MINUTE) - 
         CASE 
             WHEN delivery_mode = '60-min Express' THEN 60
             WHEN delivery_mode = '4-Hour Express' THEN 240
             WHEN customer_emirate = 'Dubai' AND EXTRACT(HOUR FROM shopify_order_datetime) < 9 THEN 720
             WHEN customer_emirate = 'Dubai' THEN 1440
             WHEN customer_emirate IN ('Abu Dhabi', 'Sharjah', 'Ajman', 'Al Ain', 'Ras Al-Khaimah') THEN 1440
             ELSE 4320
         END <= -60 THEN 1  -- Early
    WHEN DATETIME_DIFF(deliverydate, shopify_order_datetime, MINUTE) - 
         CASE 
             WHEN delivery_mode = '60-min Express' THEN 60
             WHEN delivery_mode = '4-Hour Express' THEN 240
             WHEN customer_emirate = 'Dubai' AND EXTRACT(HOUR FROM shopify_order_datetime) < 9 THEN 720
             WHEN customer_emirate = 'Dubai' THEN 1440
             WHEN customer_emirate IN ('Abu Dhabi', 'Sharjah', 'Ajman', 'Al Ain', 'Ras Al-Khaimah') THEN 1440
             ELSE 4320
         END <= 0 THEN 2  -- On-Time
    WHEN DATETIME_DIFF(deliverydate, shopify_order_datetime, MINUTE) - 
         CASE 
             WHEN delivery_mode = '60-min Express' THEN 60
             WHEN delivery_mode = '4-Hour Express' THEN 240
             WHEN customer_emirate = 'Dubai' AND EXTRACT(HOUR FROM shopify_order_datetime) < 9 THEN 720
             WHEN customer_emirate = 'Dubai' THEN 1440
             WHEN customer_emirate IN ('Abu Dhabi', 'Sharjah', 'Ajman', 'Al Ain', 'Ras Al-Khaimah') THEN 1440
             ELSE 4320
         END <= 60 THEN 3  -- Late < 1hr
    WHEN DATETIME_DIFF(deliverydate, shopify_order_datetime, MINUTE) - 
         CASE 
             WHEN delivery_mode = '60-min Express' THEN 60
             WHEN delivery_mode = '4-Hour Express' THEN 240
             WHEN customer_emirate = 'Dubai' AND EXTRACT(HOUR FROM shopify_order_datetime) < 9 THEN 720
             WHEN customer_emirate = 'Dubai' THEN 1440
             WHEN customer_emirate IN ('Abu Dhabi', 'Sharjah', 'Ajman', 'Al Ain', 'Ras Al-Khaimah') THEN 1440
             ELSE 4320
         END <= 120 THEN 4  -- Late 1-2 hrs
    ELSE 5  -- Very Late
END AS otd_category_sort,          -- dim: 1-7 sort order

-- PNA (Product Not Available) Flags
pna_flag,                          -- dim: pxna, null
pna_flag_detail,                   -- dim: resolved_pna, permanent_pna, no_pna
pna_reason,                        -- dim: no_pna, system_pna, manual_pna
pna_date,                          -- dim (datetime of PNA)

CASE 
    -- Final stages
    WHEN isdelivered IS TRUE THEN 'Delivered'
    WHEN packed = 1 THEN 'Packed'
    WHEN picked IS TRUE THEN 'Picked'
    WHEN batchcreated = 1 THEN 'Batch Created'
    WHEN batchcreated = 0 THEN 'Order Synced'
    ELSE 'Unknown'
END AS fulfillment_stage,

CASE 
    -- Completed delivery
    WHEN isdelivered IS TRUE AND returned = 0 AND iscancelled IS FALSE THEN 'Completed - Delivered'
    
    -- Returns (at different stages)
    WHEN returned = 1 AND isdelivered IS TRUE THEN 'Returned - After Delivery'
    WHEN returned = 1 THEN 'Returned - Before Delivery'
    
    -- Cancellations at different stages
    WHEN iscancelled IS TRUE AND packed = 1 THEN 'Cancelled - At Packed Stage'
    WHEN iscancelled IS TRUE AND picked IS TRUE THEN 'Cancelled - At Picked Stage'
    WHEN iscancelled IS TRUE AND batchcreated = 1 THEN 'Cancelled - At Batch Stage'
    WHEN iscancelled IS TRUE THEN 'Cancelled - At Order Stage'
    
    -- Stuck/In-Progress at different stages
    WHEN packed = 1 AND isdelivered IS FALSE AND iscancelled IS FALSE THEN 'Stuck at Delivery'
    WHEN picked IS TRUE AND packed = 0 AND iscancelled IS FALSE THEN 'Stuck at Pack'
    WHEN batchcreated = 1 AND picked IS FALSE AND iscancelled IS FALSE THEN 'Stuck at Pick'
    WHEN batchcreated = 0 AND iscancelled IS FALSE THEN 'Pending Batch'
    
    ELSE 'Unknown'
END AS fulfillment_stage_detail,


CASE 
    -- Final stages
    WHEN isdelivered IS TRUE THEN 5
    WHEN packed = 1 THEN 4
    WHEN picked IS TRUE THEN 3
    WHEN batchcreated = 1 THEN 2
    WHEN batchcreated = 0 THEN 1
    ELSE 0
END AS fulfillment_stage_sort,

    CASE
        -- Stuck at Pick: Batch created but not picked (and not cancelled)
        WHEN batchcreated = 1 
         AND picked is false 
         AND iscancelled is false 
        THEN 'Stuck at Pick'
        
        -- Stuck at Pack: Picked but not packed (and not cancelled)
        WHEN picked is true 
         AND packed = 0 
         AND iscancelled is false
        THEN 'Stuck at Pack'
        
        -- Stuck at Delivery: Packed but not delivered (and not cancelled)
        WHEN packed = 1 
         AND isdelivered is false 
         AND iscancelled is false 
        THEN 'Stuck at Delivery'
        
        -- Completed: Delivered items
        WHEN isdelivered is true  
        THEN 'Completed'
        
        -- Cancelled
        WHEN iscancelled is true  
        THEN 'Cancelled'
        
        -- Pending: Order synced but batch not created yet
        WHEN batchcreated = 0 
         AND iscancelled is false 
        THEN 'Pending Batch'
        
        -- Default
        ELSE 'Unknown'
    END AS fulfillment_stuck_stage,
    
    -- Optional: Add sort order
    CASE
        WHEN batchcreated = 0 AND iscancelled is false THEN 1  -- Pending Batch
        WHEN batchcreated = 1 AND picked is false AND iscancelled is false THEN 2  -- Stuck at Pick
        WHEN picked is true AND packed = 0 AND iscancelled is false THEN 3  -- Stuck at Pack
        WHEN packed = 1 AND isdelivered is false AND iscancelled is false THEN 4  -- Stuck at Delivery
        WHEN isdelivered = TRUE THEN 5  -- Completed
        WHEN iscancelled = TRUE THEN 6  -- Cancelled
        ELSE 7  -- Unknown
    END AS fulfillment_stuck_stage_sort,

-- Not Posted Issue Category (for revenue analysis)
-- Only applies when erp_posting_status = 'Not Posted'
CASE 
    -- PNA: Permanent PNA (highest priority)
    WHEN erp_posting_status = 'Not Posted' 
         AND pna_flag_detail = 'permanent_pna' 
        THEN 'Not Posted → PNA'
    
    -- Returns (not PNA)
    WHEN erp_posting_status = 'Not Posted' 
         AND pna_flag_detail != 'permanent_pna'
         AND crm_order_line_status IN ('ReturnClose', 'ReturnInitiated', 'ReturnedPutaway', 'ReturnQCFail', 'RetrunQCPass','ReturnGateEntry') 
        THEN 'Not Posted → Returns'
    
    -- Cancelled (not PNA)
    WHEN erp_posting_status = 'Not Posted' 
         AND pna_flag_detail != 'permanent_pna'
         AND crm_order_line_status = 'Cancel' 
        THEN 'Not Posted → Cancelled'
    
    -- Review: Everything else that's Not Posted (needs investigation)
    WHEN erp_posting_status = 'Not Posted' 
        THEN 'Not Posted → Review'
    
    -- Posted - Returns
    WHEN erp_posting_status = 'Posted' 
         AND crm_order_line_status IN ('ReturnClose', 'ReturnInitiated', 'ReturnedPutaway', 'ReturnQCFail', 'RetrunQCPass','ReturnGateEntry') 
        THEN 'Posted → Returns'
    
    -- Posted - Completed (CLOSE status)
    WHEN erp_posting_status = 'Posted' 
         AND crm_order_line_status = 'CLOSE' 
        THEN 'Posted'
    
    -- Posted - Other
    ELSE 'Posted → Other'
END AS issue_category,                 -- dim: Not Posted → PNA, Not Posted → Returns, Not Posted → Cancelled, Not Posted → Review, Posted → Returns, Posted, Posted → Other

DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at,

from source 
where 1=1
{{ dev_date_filter('ofs_order_date') }}