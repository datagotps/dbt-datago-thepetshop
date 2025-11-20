-- Trace discount issue for order O2684731, item 206216-1

-- Step 1: Check int_value_entry (earliest transformation we can check)
SELECT 
    '1. INT_VALUE_ENTRY' as data_layer,
    document_no_,
    item_no_,
    entry_type,
    item_ledger_entry_type,
    posting_date,
    invoiced_quantity,
    sales_amount__actual_,
    online_discount_amount,
    offline_discount_amount,
    online_offer_no_,
    offline_offer_no_,
    cost_amount__actual_,
    sales_channel
FROM {{ ref('int_value_entry') }}
WHERE document_no_ = 'O2684731'
ORDER BY item_no_, posting_date;

-- Step 2: Check int_order_lines
SELECT 
    '2. INT_ORDER_LINES' as data_layer,
    unified_order_id,
    document_no_,
    item_no_,
    posting_date,
    sales_amount__actual_,
    discount_amount,
    has_discount,
    sales_channel,
    transaction_type,
    discount_status
FROM {{ ref('int_order_lines') }}
WHERE unified_order_id = 'O2684731'
ORDER BY item_no_;

-- Step 3: Check what item 206216-1 is
SELECT 
    '3. ITEM INFO' as data_layer,
    item_no_,
    item_name,
    item_category,
    item_subcategory,
    division,
    inventory_posting_group
FROM {{ ref('int_items') }}
WHERE item_no_ = '206216-1';

-- Step 4: Check int_orders aggregation for this order
SELECT 
    '4. INT_ORDERS AGGREGATION' as data_layer,
    unified_order_id,
    source_no_,
    transaction_type,
    order_value,
    sales_channel,
    order_date
FROM {{ ref('int_orders') }}
WHERE unified_order_id = 'O2684731'

