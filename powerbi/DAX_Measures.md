# Power BI DAX Measures for fact_commercial Dashboard

## 📊 Core Sales Metrics

### Revenue Measures
```dax
// Total Gross Sales
Total Gross Sales =
SUM(fact_commercial[sales_amount_gross])

// Total Net Sales (After Discount)
Total Net Sales =
SUM(fact_commercial[sales_amount__actual_])

// Total Cost
Total Cost =
SUM(fact_commercial[cost_amount__actual_])

// Gross Profit
Gross Profit =
[Total Net Sales] - [Total Cost]

// Gross Profit Margin %
Gross Profit Margin % =
DIVIDE([Gross Profit], [Total Net Sales], 0) * 100

// Average Order Value
Average Order Value =
DIVIDE([Total Net Sales], DISTINCTCOUNT(fact_commercial[document_no_]), 0)

// Average Transaction Value
Average Transaction Value =
DIVIDE([Total Net Sales], COUNTROWS(fact_commercial), 0)
```

### Quantity Metrics
```dax
// Total Quantity Sold
Total Quantity Sold =
SUM(fact_commercial[invoiced_quantity])

// Total Orders
Total Orders =
DISTINCTCOUNT(fact_commercial[document_no_])

// Total Transactions
Total Transactions =
COUNTROWS(fact_commercial)

// Average Quantity Per Order
Avg Quantity Per Order =
DIVIDE([Total Quantity Sold], [Total Orders], 0)
```

## 💰 Discount Analysis

```dax
// Total Discount Amount
Total Discount =
SUM(fact_commercial[discount_amount])

// Discount Rate %
Discount Rate % =
DIVIDE([Total Discount], [Total Gross Sales], 0) * 100

// Online Discount
Online Discount =
SUM(fact_commercial[online_discount_amount])

// Offline Discount
Offline Discount =
SUM(fact_commercial[offline_discount_amount])

// Discounted Orders Count
Discounted Orders =
CALCULATE(
    DISTINCTCOUNT(fact_commercial[document_no_]),
    fact_commercial[has_discount] = 1
)

// Discounted Orders %
Discounted Orders % =
DIVIDE([Discounted Orders], [Total Orders], 0) * 100

// Average Discount Per Discounted Order
Avg Discount Per Order =
DIVIDE([Total Discount], [Discounted Orders], 0)
```

## 🛒 Sales Channel Analysis

```dax
// Online Sales
Online Sales =
CALCULATE(
    [Total Net Sales],
    fact_commercial[sales_channel] = "Online"
)

// Offline Sales
Offline Sales =
CALCULATE(
    [Total Net Sales],
    fact_commercial[sales_channel] IN {"Shop", "B2B"}
)

// Online Sales %
Online Sales % =
DIVIDE([Online Sales], [Total Net Sales], 0) * 100

// Channel Mix %
Channel Mix % =
DIVIDE([Total Net Sales],
    CALCULATE([Total Net Sales], ALL(fact_commercial[sales_channel])),
    0) * 100
```

## 📅 Time Intelligence Measures

```dax
// MTD Sales
MTD Sales =
CALCULATE(
    [Total Net Sales],
    fact_commercial[is_mtd] = 1
)

// YTD Sales
YTD Sales =
CALCULATE(
    [Total Net Sales],
    fact_commercial[is_ytd] = 1
)

// Last Month Sales
Last Month Sales =
CALCULATE(
    [Total Net Sales],
    fact_commercial[is_m_1] = 1
)

// Last Year Sales
Last Year Sales =
CALCULATE(
    [Total Net Sales],
    fact_commercial[is_y_1] = 1
)

// YoY Growth %
YoY Growth % =
VAR CurrentYearSales = [YTD Sales]
VAR LastYearSales = CALCULATE(
    [Total Net Sales],
    fact_commercial[is_lytd] = 1
)
RETURN
DIVIDE(CurrentYearSales - LastYearSales, LastYearSales, 0) * 100

// MoM Growth %
MoM Growth % =
VAR CurrentMonth = [MTD Sales]
VAR LastMonth = CALCULATE(
    [Total Net Sales],
    fact_commercial[is_lmtd] = 1
)
RETURN
DIVIDE(CurrentMonth - LastMonth, LastMonth, 0) * 100

// Same Period Last Year
SPLY Sales =
CALCULATE(
    [Total Net Sales],
    fact_commercial[is_lymtd] = 1
)
```

## 👥 Customer Analytics

```dax
// Total Customers
Total Customers =
DISTINCTCOUNT(fact_commercial[unified_customer_id])

// New Customers (First Purchase)
New Customers =
CALCULATE(
    DISTINCTCOUNT(fact_commercial[unified_customer_id]),
    FILTER(
        fact_commercial,
        fact_commercial[posting_date] =
        CALCULATE(
            MIN(fact_commercial[posting_date]),
            ALLEXCEPT(fact_commercial, fact_commercial[unified_customer_id])
        )
    )
)

// Repeat Customers
Repeat Customers =
[Total Customers] - [New Customers]

// Repeat Rate %
Repeat Rate % =
DIVIDE([Repeat Customers], [Total Customers], 0) * 100

// Customer Lifetime Value
Customer Lifetime Value =
DIVIDE([Total Net Sales], [Total Customers], 0)

// Average Orders Per Customer
Avg Orders Per Customer =
DIVIDE([Total Orders], [Total Customers], 0)

// Loyalty Members
Loyalty Members =
CALCULATE(
    DISTINCTCOUNT(fact_commercial[unified_customer_id]),
    NOT(ISBLANK(fact_commercial[loyality_member_id]))
)

// Loyalty Penetration %
Loyalty Penetration % =
DIVIDE([Loyalty Members], [Total Customers], 0) * 100

// Verified Customers
Verified Customers =
CALCULATE(
    DISTINCTCOUNT(fact_commercial[unified_customer_id]),
    fact_commercial[customer_identity_status] = "Verified"
)
```

## 🏪 Location Performance

```dax
// Location Sales Ranking
Location Rank =
RANKX(
    ALL(fact_commercial[location_code]),
    [Total Net Sales],
    ,
    DESC,
    DENSE
)

// Location Sales Contribution %
Location Contribution % =
DIVIDE(
    [Total Net Sales],
    CALCULATE([Total Net Sales], ALL(fact_commercial[location_code])),
    0
) * 100

// Top Location Indicator
Is Top Location =
IF([Location Rank] <= 5, "Top 5", "Others")
```

## 📦 Product Performance

```dax
// Product SKU Count
Active SKUs =
DISTINCTCOUNT(fact_commercial[item_no_])

// Product Sales Ranking
Product Rank =
RANKX(
    ALL(fact_commercial[item_no_]),
    [Total Net Sales],
    ,
    DESC,
    DENSE
)

// Category Contribution %
Category Contribution % =
DIVIDE(
    [Total Net Sales],
    CALCULATE([Total Net Sales], ALL(fact_commercial[item_category])),
    0
) * 100

// Average Price Per Unit
Avg Price Per Unit =
DIVIDE([Total Net Sales], [Total Quantity Sold], 0)

// Average Cost Per Unit
Avg Cost Per Unit =
DIVIDE([Total Cost], [Total Quantity Sold], 0)

// Division Performance
Division Sales % =
DIVIDE(
    [Total Net Sales],
    CALCULATE([Total Net Sales], ALL(fact_commercial[division])),
    0
) * 100
```

## 💳 Payment & Transaction Type

```dax
// Sales by Transaction Type
Sales by Transaction Type =
CALCULATE(
    [Total Net Sales],
    VALUES(fact_commercial[transaction_type])
)

// Refund Amount
Refund Amount =
CALCULATE(
    [Total Net Sales],
    fact_commercial[transaction_type] = "Refund"
)

// Net Sales (Sales - Refunds)
Net Sales After Refunds =
CALCULATE([Total Net Sales], fact_commercial[transaction_type] = "Sale") +
CALCULATE([Total Net Sales], fact_commercial[transaction_type] = "Refund")

// Refund Rate %
Refund Rate % =
DIVIDE(
    ABS([Refund Amount]),
    CALCULATE([Total Net Sales], fact_commercial[transaction_type] = "Sale"),
    0
) * 100

// COD Orders
COD Orders =
CALCULATE(
    [Total Orders],
    fact_commercial[paymentmethodcode] = "COD"
)

// Prepaid Orders
Prepaid Orders =
CALCULATE(
    [Total Orders],
    fact_commercial[paymentmethodcode] = "PREPAID"
)

// COD Rate %
COD Rate % =
DIVIDE([COD Orders], [Total Orders], 0) * 100
```

## 🎯 KPI Targets & Variance

```dax
// Sales Target (Set your target)
Sales Target = 10000000

// Sales vs Target
Sales vs Target =
[Total Net Sales] - [Sales Target]

// Target Achievement %
Target Achievement % =
DIVIDE([Total Net Sales], [Sales Target], 0) * 100

// Target Status
Target Status =
SWITCH(
    TRUE(),
    [Target Achievement %] >= 100, "✅ Achieved",
    [Target Achievement %] >= 90, "⚠️ Near Target",
    "❌ Below Target"
)
```

## 📊 Advanced Analytics

```dax
// Running Total Sales
Running Total Sales =
CALCULATE(
    [Total Net Sales],
    FILTER(
        ALL(fact_commercial[posting_date]),
        fact_commercial[posting_date] <= MAX(fact_commercial[posting_date])
    )
)

// Sales Growth Index (Base = 100)
Sales Growth Index =
VAR BaselineSales = CALCULATE([Total Net Sales], ALL(fact_commercial[posting_date]))
RETURN
DIVIDE([Total Net Sales], BaselineSales, 0) * 100

// Contribution to Total %
Contribution to Total % =
DIVIDE(
    [Total Net Sales],
    CALCULATE([Total Net Sales], ALL(fact_commercial)),
    0
) * 100

// ABC Classification (Product)
ABC Classification =
VAR CumulativeContribution =
    CALCULATE(
        [Contribution to Total %],
        FILTER(
            ALL(fact_commercial[item_no_]),
            [Product Rank] <= EARLIER([Product Rank])
        )
    )
RETURN
SWITCH(
    TRUE(),
    CumulativeContribution <= 80, "A - Top 80%",
    CumulativeContribution <= 95, "B - Next 15%",
    "C - Bottom 5%"
)

// Basket Size
Basket Size =
[Average Quantity Per Order]

// Conversion Rate (if you have traffic data)
// Conversion Rate % = DIVIDE([Total Orders], [Total Traffic], 0) * 100
```

## 🔄 Dynamic Measures

```dax
// Dynamic Measure Selector
Selected Measure =
SWITCH(
    SELECTEDVALUE('Measure Selector'[Measure]),
    "Net Sales", [Total Net Sales],
    "Gross Sales", [Total Gross Sales],
    "Quantity", [Total Quantity Sold],
    "Profit", [Gross Profit],
    "Orders", [Total Orders],
    [Total Net Sales]
)

// Dynamic Time Period (MTD, QTD, YTD)
Dynamic Time Sales =
SWITCH(
    SELECTEDVALUE('Period Selector'[Period]),
    "MTD", [MTD Sales],
    "YTD", [YTD Sales],
    "Last Month", [Last Month Sales],
    "Last Year", [Last Year Sales],
    [Total Net Sales]
)
```

## 🎨 Conditional Formatting Helpers

```dax
// Profit Color
Profit Color =
IF([Gross Profit] > 0, "Green", "Red")

// Growth Indicator
Growth Indicator =
SWITCH(
    TRUE(),
    [YoY Growth %] > 10, "🟢 Strong Growth",
    [YoY Growth %] > 0, "🟡 Moderate Growth",
    [YoY Growth %] >= -5, "🟠 Slight Decline",
    "🔴 Significant Decline"
)

// Performance Rating
Performance Rating =
VAR Score = [Target Achievement %]
RETURN
SWITCH(
    TRUE(),
    Score >= 120, "⭐⭐⭐⭐⭐ Outstanding",
    Score >= 100, "⭐⭐⭐⭐ Excellent",
    Score >= 90, "⭐⭐⭐ Good",
    Score >= 75, "⭐⭐ Fair",
    "⭐ Needs Improvement"
)
```

## 📈 Forecasting (Simple Moving Average)

```dax
// 7-Day Moving Average Sales
7-Day Moving Avg =
CALCULATE(
    [Total Net Sales],
    DATESINPERIOD(
        fact_commercial[posting_date],
        LASTDATE(fact_commercial[posting_date]),
        -7,
        DAY
    )
) / 7

// 30-Day Moving Average Sales
30-Day Moving Avg =
CALCULATE(
    [Total Net Sales],
    DATESINPERIOD(
        fact_commercial[posting_date],
        LASTDATE(fact_commercial[posting_date]),
        -30,
        DAY
    )
) / 30
```

---

## 🚀 Implementation Notes

1. **Create these measures in Power BI Desktop** in the fact_commercial table or a separate measures table
2. **Use appropriate formatting**:
   - Currency: Total Net Sales, costs, profits (AED format)
   - Percentages: All % measures (1 decimal place)
   - Whole numbers: Quantities, counts
3. **Create calculation groups** for time intelligence if needed
4. **Use field parameters** for dynamic measure selection
5. **Hide unused columns** in report view to keep it clean
6. **Create measure folders** to organize these by category
