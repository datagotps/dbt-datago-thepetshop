-- Layer 1: int_value_entry for order O2684731
SELECT 
    document_no_,
    item_no_,
    sales_channel,
    posting_date,
    invoiced_quantity,
    sales_amount__actual_,
    online_discount_amount,
    offline_discount_amount,
    online_offer_no_,
    offline_offer_no_
FROM {{ ref('int_value_entry') }}
WHERE document_no_ = 'O2684731'
ORDER BY item_no_

