-- Layer 2: int_order_lines for order O2684731
SELECT 
    unified_order_id,
    document_no_,
    item_no_,
    item_name,
    sales_channel,
    posting_date,
    invoiced_quantity,
    sales_amount__actual_,
    discount_amount,
    has_discount,
    discount_status,
    online_offer_no_,
    offline_offer_no_
FROM {{ ref('int_order_lines') }}
WHERE unified_order_id = 'O2684731'
ORDER BY ABS(discount_amount) DESC

