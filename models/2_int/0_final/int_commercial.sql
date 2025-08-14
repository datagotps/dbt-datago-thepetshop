with inbound_sales_line_dedup AS (
    SELECT 
        documentno,
        item_no_,
        -- Aggregate other needed columns
      --  MAX(web_order_no_) as web_order_no_,
       -- MAX(customer_no_) as customer_no_,
      --  SUM(quantity) as total_quantity,
      --  MAX(invoice_value_incl__tax) as invoice_value_incl__tax,
      --  MAX(unit_price_excl__vat) as unit_price_excl__vat,
        MAX(discount_amount) as discount_amount,
      --  MAX(coupon_discount) as coupon_discount,
      --  MAX(tax_amount) as tax_amount,
      --  MAX(type) as type,
      --  COUNT(*) as line_count  -- Track how many lines were aggregated
    FROM {{ ref('stg_erp_inbound_sales_line') }}
    GROUP BY documentno, item_no_
) 


select

a.source_no_,
a.document_no_,
a.posting_date,

a.invoiced_quantity,
a.sales_channel,
a.transaction_type,
    -- Alternative: You could also create a text indicator
    CASE 
        WHEN a.sales_channel = 'Online' AND f.discount_amount IS NOT NULL AND f.discount_amount != 0 THEN 'Discounted'
        WHEN a.sales_channel != 'Online' AND a.discount_amount IS NOT NULL AND a.discount_amount != 0 THEN 'Discounted'
        ELSE 'No Discount'
    END AS discount_status,

    -- GROSS SALES AMOUNT CALCULATION
    -- For Sales: Net sales + absolute value of discount = Gross sales
    -- For Refunds: Net refund - discount = Gross refund (more negative)
    -- Using ROUND to avoid floating point precision issues
    ROUND(
        CASE 
            WHEN a.transaction_type = 'Refund' THEN 
                -- For refunds: subtract the discount to get the original gross amount (more negative)
                a.sales_amount__actual_ - ABS(
                    CASE 
                        WHEN a.sales_channel = 'Online' THEN ROUND(COALESCE(-1*f.discount_amount,0) / (1 + 5 / 100), 2)
                        ELSE COALESCE(a.discount_amount, 0)
                    END
                )
            ELSE 
                -- For sales: add the discount to get the original gross amount
                a.sales_amount__actual_ + ABS(
                    CASE 
                        WHEN a.sales_channel = 'Online' THEN ROUND(COALESCE(-1*f.discount_amount,0) / (1 + 5 / 100), 2)
                        ELSE COALESCE(a.discount_amount, 0)
                    END
                )
        END, 2
    ) AS sales_amount_gross,


    -- CONSOLIDATED DISCOUNT AMOUNT
    -- Use online discount for Online sales channel, otherwise use standard discount
    CASE 
        WHEN a.sales_channel = 'Online' THEN ROUND(COALESCE(-1*f.discount_amount,0) / (1 + 5 / 100), 2)
        ELSE COALESCE(a.discount_amount, 0)
    END AS discount_amount,

a.sales_amount__actual_,
a.cost_amount__actual_,
a.discount_amount as offline_discount_amount,
ROUND(COALESCE(-1*f.discount_amount,0) / (1 + 5 / 100), 2) as online_discount_amount,


a.document_type,




    
    -- DISCOUNT INDICATOR FLAG
    -- Check if any discount exists (non-zero value)
    CASE 
        WHEN a.sales_channel = 'Online' AND f.discount_amount IS NOT NULL AND f.discount_amount != 0 THEN 1
        WHEN a.sales_channel != 'Online' AND a.discount_amount IS NOT NULL AND a.discount_amount != 0 THEN 1
        ELSE 0
    END AS has_discount,
    



 
a.sales_channel_sort,

a.offline_order_channel, --store location
a.source_code,
a.item_ledger_entry_type,

        -- Posting Groups
        a.gen__prod__posting_group,
        a.gen__bus__posting_group,
        a.source_posting_group,
        a.inventory_posting_group,
a.global_dimension_1_code,
a.global_dimension_2_code,
a.location_code,
a.clc_location_code,
a.user_id,
a.entry_type_2,

a.dimension_code,
a.global_dimension_2_code_name,
a.clc_global_dimension_2_code_name,

--case when transaction_type = 'Sale' then a.sales_amount__actual_ else 0 end as posted_revenue,
--case when transaction_type = 'Refund' then a.sales_amount__actual_ else 0 end as refund,


case 
when a.sales_channel = 'Shop' then a.location_code 
when a.sales_channel = 'Online' then b.online_order_channel
else a.global_dimension_2_code_name end as sales_channel_detail,


CASE 
    WHEN a.sales_channel = 'Online' THEN b.web_order_id 
    WHEN a.sales_channel IN ('Shop', 'Affiliate', 'B2B', 'Service') AND a.transaction_type = 'Sale' THEN a.document_no_
    ELSE NULL 
END AS unified_order_id,

CASE WHEN a.transaction_type = 'Refund' THEN a.document_no_  ELSE null  END AS unified_refund_id,


b.web_order_id,
b.online_order_channel, --website, Android, iOS, CRM, Unmapped
b.order_type, --EXPRESS, NORMAL, EXCHANGE
b.paymentgateway, -- creditCard, cash, CreditCard, Cash On Delivery, Cash, Pay by Card, StoreCredit, Card on delivery, Cash on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)
b.paymentmethodcode, -- PREPAID, COD, creditCard


c.name,
c.raw_phone_no_,
c.customer_identity_status,


e.item_no_,
e.item_name,
e.item_category,
e.item_subcategory,
e.item_brand,
e.division,

--case when a.source_code != 'INVTADJMT' and a.sales_channel = 'Online' and a.document_type_2 != 'Sales Credit Memo' then b.amount else 0 end as online_line_amount_excl_vat,
--case when a.source_code != 'INVTADJMT' and a.sales_channel = 'Online' and a.document_type_2 != 'Sales Credit Memo' then b.amount_including_vat else 0 end as online_line_amount_enclu_vat,

--d.name,
--count(*)

    -- Current Period Flags (based on posting_date)
    CASE WHEN a.posting_date >= DATE_TRUNC(DATE(CURRENT_DATETIME()), MONTH) 
         AND a.posting_date <= CURRENT_DATE() 
         THEN 1 ELSE 0 END as is_mtd,
    
    CASE WHEN a.posting_date >= DATE_TRUNC(DATE(CURRENT_DATETIME()), YEAR) 
         AND a.posting_date <= CURRENT_DATE() 
         THEN 1 ELSE 0 END as is_ytd,
    
    -- Last Month To Date Flag
    CASE WHEN a.posting_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
         AND a.posting_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
         AND EXTRACT(DAY FROM a.posting_date) <= EXTRACT(DAY FROM CURRENT_DATE())
         THEN 1 ELSE 0 END as is_lmtd,
    
    -- Last Year Month To Date Flag
    CASE WHEN a.posting_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH)
         AND a.posting_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
         AND EXTRACT(DAY FROM a.posting_date) <= EXTRACT(DAY FROM CURRENT_DATE())
         AND EXTRACT(MONTH FROM a.posting_date) = EXTRACT(MONTH FROM CURRENT_DATE())
         THEN 1 ELSE 0 END as is_lymtd,
    
    -- Last Year To Date Flag
    CASE WHEN a.posting_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
         AND a.posting_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
         THEN 1 ELSE 0 END as is_lytd,


-- M_1: Last Month (Full Month) - If current = August, M_1 = July (full month)
CASE WHEN DATE_TRUNC(a.posting_date, MONTH) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
     THEN 1 ELSE 0 END as is_m_1,

-- M_2: Two Months Ago (Full Month) - If current = August, M_2 = June (full month)
CASE WHEN DATE_TRUNC(a.posting_date, MONTH) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH)
     THEN 1 ELSE 0 END as is_m_2,

-- M_3: Three Months Ago (Full Month) - If current = August, M_3 = May (full month)
CASE WHEN DATE_TRUNC(a.posting_date, MONTH) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), MONTH)
     THEN 1 ELSE 0 END as is_m_3,

CASE WHEN DATE_TRUNC(a.posting_date, YEAR) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
     THEN 1 ELSE 0 END as is_y_1,

CASE WHEN DATE_TRUNC(a.posting_date, YEAR) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR), YEAR)
     THEN 1 ELSE 0 END as is_y_2,



FROM {{ ref('stg_erp_value_entry') }} as a
LEFT JOIN  {{ ref('stg_erp_inbound_sales_header') }}  as b on a.document_no_ = b.documentno
LEFT JOIN {{ ref('int_erp_customer') }} AS c ON a.source_no_ = c.no_

left join {{ ref('stg_erp_sales_invoice_line') }} as d on a.document_no_ = d.document_no_ and a.source_code = 'SALES' and a.document_line_no_ = d.line_no_

--left join {{ ref('stg_erp_inbound_sales_line') }} as f on d.document_no_ = f.documentno and d.sell_to_customer_no_ = f.customer_no_ 
LEFT JOIN inbound_sales_line_dedup as f ON a.document_no_ = f.documentno AND a.item_no_ = f.item_no_


--left join {{ ref('stg_dimension_value') }} as d on  a.global_dimension_2_code = d.code

left join  {{ ref('int_items') }} as e on  e.item_no_ = a.item_no_





where a.item_ledger_entry_type = 'Sale' 
and a.source_code  IN ('BACKOFFICE', 'SALES')
and a.document_type NOT IN ('Sales Shipment', 'Sales Return Receipt')
and a.dimension_code = 'PROFITCENTER'


and a.posting_date BETWEEN '2025-01-01' AND '2025-01-31'


--and a.transaction_type != 'Sale'
--and a.posting_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 25 MONTH) AND CURRENT_DATE()

--and   b.web_order_id = 'O3067781S'