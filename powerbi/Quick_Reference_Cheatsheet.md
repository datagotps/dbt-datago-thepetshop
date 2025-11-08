# ⚡ Power BI Dashboard Quick Reference
## The Pet Shop Commercial Analytics - Cheatsheet

---

## 🎯 Top 10 Most Important Measures

```dax
// 1. Total Net Sales (After Discount)
Total Net Sales = SUM(fact_commercial[sales_amount__actual_])

// 2. Gross Profit
Gross Profit = [Total Net Sales] - SUM(fact_commercial[cost_amount__actual_])

// 3. Gross Profit Margin %
Gross Profit Margin % = DIVIDE([Gross Profit], [Total Net Sales], 0) * 100

// 4. Total Orders
Total Orders = DISTINCTCOUNT(fact_commercial[document_no_])

// 5. Total Customers
Total Customers = DISTINCTCOUNT(fact_commercial[unified_customer_id])

// 6. Average Order Value
Average Order Value = DIVIDE([Total Net Sales], [Total Orders], 0)

// 7. YoY Growth %
YoY Growth % =
VAR CurrentYearSales = [Total Net Sales]
VAR LastYearSales = CALCULATE([Total Net Sales], fact_commercial[is_y_1] = 1)
RETURN DIVIDE(CurrentYearSales - LastYearSales, LastYearSales, 0) * 100

// 8. Total Discount
Total Discount = SUM(fact_commercial[discount_amount])

// 9. Discount Rate %
Discount Rate % = DIVIDE([Total Discount], SUM(fact_commercial[sales_amount_gross]), 0) * 100

// 10. Online Sales %
Online Sales % =
DIVIDE(
    CALCULATE([Total Net Sales], fact_commercial[sales_channel] = "Online"),
    [Total Net Sales],
    0
) * 100
```

---

## 📊 Essential Visuals by Page

### Page 1: Executive Summary
- ✅ 4 KPI Cards (Sales, Profit, Orders, Customers)
- ✅ 3 Gauge Charts (Target achievement)
- ✅ Line Chart (Sales trend)
- ✅ Donut Chart (Channel mix)
- ✅ 3 Bar Charts (Top performers)

### Page 2: Sales Performance
- ✅ Waterfall Chart (Sales breakdown)
- ✅ MTD vs LMTD comparison
- ✅ Sales by channel matrix
- ✅ Daily trend with moving averages

### Page 3: Customer Analytics
- ✅ Customer segment breakdown
- ✅ New vs Repeat customers
- ✅ Customer Lifetime Value
- ✅ Top customers table

### Page 4: Product Performance
- ✅ Scatter chart (Qty vs Margin)
- ✅ Top categories/brands
- ✅ ABC classification
- ✅ Product performance table

---

## 🎨 Color Codes (Copy-Paste Ready)

### Primary Colors
```
Blue (Primary):     #0078D4
Green (Success):    #107C10
Orange (Warning):   #FF8C00
Red (Alert):        #D13438
Gray (Neutral):     #605E5C
```

### Light Colors (Backgrounds)
```
Light Blue:    #DEECF9
Light Green:   #DFF6DD
Light Orange:  #FFF4CE
Light Red:     #FDE7E9
Background:    #FAFAFA
```

---

## ⌨️ Keyboard Shortcuts

```
Ctrl + S              Save
Ctrl + Shift + M      New Measure
Ctrl + C / V          Copy / Paste
Ctrl + Z / Y          Undo / Redo
Ctrl + F              Find
Ctrl + G              Group visuals
Ctrl + Shift + C      Format painter
Alt + Shift + Arrow   Duplicate visual
F11                   Full screen
```

---

## 🔧 Common DAX Patterns

### Pattern 1: CALCULATE with Filter
```dax
Measure Name =
CALCULATE(
    [Base Measure],
    fact_commercial[column] = "value"
)
```

### Pattern 2: DIVIDE (Safe Division)
```dax
Rate % = DIVIDE([Numerator], [Denominator], 0) * 100
```

### Pattern 3: Time Intelligence
```dax
Last Year = CALCULATE([Measure], fact_commercial[is_y_1] = 1)
```

### Pattern 4: Using Variables
```dax
Measure =
VAR Variable1 = [Some Measure]
VAR Variable2 = [Another Measure]
RETURN Variable1 - Variable2
```

### Pattern 5: SWITCH for Multiple Conditions
```dax
Status =
SWITCH(
    TRUE(),
    [Value] >= 100, "High",
    [Value] >= 50, "Medium",
    "Low"
)
```

---

## 📝 Measure Formatting Quick Guide

### Format Strings
```dax
// Currency (AED)
Format: "Currency"
Currency: AED
Decimal Places: 2

// Percentage
Format: "Percentage"
Decimal Places: 1

// Whole Number
Format: "Whole Number"
Thousands Separator: Yes

// Custom
#,##0.0                  // One decimal with thousands separator
"AED "#,##0.00           // Currency with symbol
0.0"%"                   // Percentage with one decimal
```

---

## 🔍 Filter Context Helpers

```dax
// Remove all filters from a table
ALL(fact_commercial)

// Remove filters from specific columns
ALL(fact_commercial[sales_channel])

// Keep only specific columns in filter context
ALLEXCEPT(fact_commercial, fact_commercial[posting_date])

// Get all values from a column
VALUES(fact_commercial[sales_channel])

// Check if specific value is selected
SELECTEDVALUE(fact_commercial[sales_channel])

// Count of selected items
COUNTROWS(VALUES(fact_commercial[sales_channel]))
```

---

## 📅 Time Intelligence Quick Reference

### Using Built-in Flags
```dax
MTD Sales = CALCULATE([Total Net Sales], fact_commercial[is_mtd] = 1)
YTD Sales = CALCULATE([Total Net Sales], fact_commercial[is_ytd] = 1)
Last Month = CALCULATE([Total Net Sales], fact_commercial[is_m_1] = 1)
Last Year = CALCULATE([Total Net Sales], fact_commercial[is_y_1] = 1)
LMTD = CALCULATE([Total Net Sales], fact_commercial[is_lmtd] = 1)
LYTD = CALCULATE([Total Net Sales], fact_commercial[is_lytd] = 1)
```

---

## 🎯 Conditional Formatting Rules

### Data Bars
- Good for: Showing relative magnitude
- Use on: Sales columns, quantity columns
- Colors: Blue (#0078D4) for positive values

### Color Scales
- Good for: Heatmaps, performance matrices
- Use on: Growth %, margins, performance scores
- Colors: Red → Yellow → Green gradient

### Icons
- Good for: Status indicators
- Rules:
  - 🔴 Red: < 75% or negative
  - 🟡 Yellow: 75-100%
  - 🟢 Green: > 100%

### Background Color (DAX)
```dax
Profit Color =
IF([Gross Profit] > 0, "#107C10", "#D13438")
```

---

## 🚀 Performance Tips

### DO ✅
- Use measures instead of calculated columns
- Use variables in DAX for reusable calculations
- Hide unused fields
- Limit visuals to 15-20 per page
- Use integers for IDs instead of text
- Create aggregation tables for large datasets

### DON'T ❌
- Don't use calculated columns for aggregations
- Don't use bidirectional filters unless necessary
- Don't show all rows in table visuals
- Don't use complex DAX in calculated columns
- Don't ignore Performance Analyzer warnings
- Don't forget to hide backend tables

---

## 🔐 Row-Level Security (RLS) Templates

### Template 1: User-based
```dax
[user_id] = USERPRINCIPALNAME()
```

### Template 2: Location-based
```dax
[location_code] IN VALUES(UserAccess[AllowedLocations])
```

### Template 3: Company-based
```dax
[company_source] = "Petshop"
```

### Testing RLS
1. **Modeling** → **View as Roles**
2. Select role to test
3. Enter username (if dynamic)
4. Click OK
5. Verify filtered data

---

## 🎨 Visual Best Practices

### Card Visuals
- Font Size: 32-40pt for value
- Font Size: 10-12pt for label
- Padding: 15-20px
- Background: White
- Border: 1px, light gray

### Line Charts
- Max lines: 3-4
- Line width: 2-3px
- Show legend: If > 1 line
- Markers: Optional (use for < 20 points)

### Bar Charts
- Sort: By value (descending)
- Show values: On bars if < 10 bars
- Color: Single color unless comparing groups

### Tables
- Max rows: 20 (use pagination for more)
- Conditional formatting: On key metrics
- Grid: Horizontal lines only
- Font: 10-11pt

---

## 📱 Mobile Layout Tips

### Do's
- Portrait orientation (9:16)
- Max 2-3 visuals per screen
- Larger font sizes (+20%)
- Touch targets ≥ 44x44 pixels
- Simplified visuals

### Don'ts
- Complex matrices
- Small slicers
- Too many filters
- Horizontal scrolling

---

## 🆘 Quick Troubleshooting

| Problem | Solution |
|---------|----------|
| Blank visual | Check relationship, verify data exists |
| Wrong totals | Review filter context, check CALCULATE |
| Slow report | Use Performance Analyzer, reduce visuals |
| Refresh failed | Check credentials, verify query timeout |
| Circular dependency | Review measure references, break cycle |
| Date hierarchy missing | Mark date table, verify relationship |
| RLS not working | Test with "View as", check DAX syntax |

---

## 📊 Data Model Health Check

```dax
// Check row count
Total Rows = COUNTROWS(fact_commercial)

// Check distinct values (for cardinality)
Distinct Customers = DISTINCTCOUNT(fact_commercial[unified_customer_id])
Distinct Products = DISTINCTCOUNT(fact_commercial[item_no_])
Distinct Locations = DISTINCTCOUNT(fact_commercial[location_code])

// Check for blanks
Blank Customers = CALCULATE(COUNTROWS(fact_commercial), ISBLANK(fact_commercial[unified_customer_id]))

// Check date range
Min Date = MIN(fact_commercial[posting_date])
Max Date = MAX(fact_commercial[posting_date])
```

---

## 🔄 Refresh Schedule Best Practices

### Recommended Schedule
- **Daily refresh**: 6:00 AM (before business hours)
- **Frequency**: Once per day (unless real-time needed)
- **Timezone**: Local business timezone
- **Notifications**: Enable failure alerts

### Incremental Refresh Setup
```
Archive data starting: 2 years before refresh date
Incrementally refresh: 7 days before refresh date
Detect data changes: Yes (posting_date column)
```

---

## 📞 Quick Links

- **Power BI Service**: https://app.powerbi.com
- **DAX Guide**: https://dax.guide
- **Color Picker**: https://htmlcolorcodes.com
- **Icon Library**: Power BI Icons (built-in)
- **Community**: https://community.powerbi.com

---

## ✅ Pre-Publish Checklist

Before publishing to Power BI Service:

- [ ] All measures working correctly
- [ ] Visuals showing expected data
- [ ] Filters and slicers configured
- [ ] Mobile layout created
- [ ] Report tested with different data ranges
- [ ] Performance acceptable (< 5 sec load)
- [ ] Unnecessary fields hidden
- [ ] Proper formatting applied
- [ ] Tooltips configured
- [ ] Navigation working
- [ ] RLS configured (if needed)
- [ ] File saved with clear name

---

**Quick Tip**: Print this cheatsheet and keep it handy while building your dashboard!

---

**Version**: 1.0
**Last Updated**: 2025-11-08
