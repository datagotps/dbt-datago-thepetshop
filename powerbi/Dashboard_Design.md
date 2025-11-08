# 🎨 Power BI Interactive Dashboard Design
## The Pet Shop Commercial Analytics Dashboard

---

## 📋 Dashboard Overview

**Purpose**: Provide comprehensive business intelligence for The Pet Shop's commercial operations across all sales channels, locations, and product categories.

**Target Users**:
- Executive Leadership (C-Suite)
- Sales Managers
- Marketing Teams
- Operations Managers
- Finance Teams

**Update Frequency**: Real-time (based on dbt model refresh)

---

## 🏗️ Dashboard Architecture

### Navigation Structure
```
📊 Main Navigation (Bookmarks/Buttons)
├── 🏠 Executive Summary
├── 💰 Sales Performance
├── 👥 Customer Analytics
├── 📦 Product Performance
├── 🏪 Location & Channel Analysis
├── 💳 Payment & Transactions
├── 🎁 Discount Analysis
└── 📅 Time Trends
```

---

## 📄 Page 1: Executive Summary

### Layout: 3 Columns

#### Top Row - KPI Cards (4 cards)
```
┌─────────────┬─────────────┬─────────────┬─────────────┐
│ Total Sales │ Gross Profit│ Total Orders│  Customers  │
│  AED 25.5M  │  AED 8.2M   │   45,234    │   12,456    │
│ ↑ 15.3% YoY │ ↑ 12.1% YoY │ ↑ 18.5% YoY │ ↑ 8.2% YoY  │
└─────────────┴─────────────┴─────────────┴─────────────┘
```
**Visuals**: Card visuals with conditional formatting
**Measures**:
- Total Net Sales, YoY Growth %
- Gross Profit, YoY Growth %
- Total Orders, YoY Growth %
- Total Customers, YoY Growth %

#### Second Row - Performance Gauges (3 gauges)
```
┌──────────────┬──────────────┬──────────────┐
│ Sales Target │ Profit Target│ Loyalty Rate │
│   Gauge      │    Gauge     │    Gauge     │
│    95%       │    102%      │     68%      │
└──────────────┴──────────────┴──────────────┘
```
**Visuals**: Gauge charts
**Measures**:
- Target Achievement %
- Profit Margin vs Target
- Loyalty Penetration %

#### Third Row - Trends & Mix
```
┌────────────────────────────┬──────────────────────────┐
│  Sales Trend (Line Chart)  │  Channel Mix (Donut)    │
│  Monthly - Last 12 Months  │  Online vs Offline      │
│  With 30-Day Moving Avg    │  With % Labels          │
└────────────────────────────┴──────────────────────────┘
```

#### Fourth Row - Top Performers
```
┌──────────────────┬──────────────────┬──────────────────┐
│ Top 5 Products   │ Top 5 Locations  │ Top 5 Categories│
│ (Bar Chart)      │ (Bar Chart)      │ (Bar Chart)     │
└──────────────────┴──────────────────┴──────────────────┘
```

#### Global Filters (Right Panel)
```
📅 Date Range Picker
🏢 Company Source (Petshop/Pethaus)
🛒 Sales Channel
📍 Location City
🔄 Transaction Type
```

---

## 📄 Page 2: Sales Performance Deep Dive

### Layout: Grid Layout

#### Top Row - Sales KPIs (5 cards)
```
┌────────────┬────────────┬────────────┬────────────┬────────────┐
│ Gross Sales│  Net Sales │   Profit   │   Margin   │    AOV     │
│ AED 28.2M  │ AED 25.5M  │ AED 8.2M   │   32.2%    │  AED 564   │
└────────────┴────────────┴────────────┴────────────┴────────────┘
```

#### Main Section - Sales Waterfall
```
┌────────────────────────────────────────────────────┐
│          Sales Waterfall Chart                     │
│  Gross Sales → Discounts → Net Sales → Cost       │
│  → Gross Profit                                    │
└────────────────────────────────────────────────────┘
```

#### Left Column - Time Analysis
```
┌────────────────────────────┐
│  MTD vs LMTD Comparison    │
│  (Clustered Bar Chart)     │
├────────────────────────────┤
│  YTD vs LYTD Comparison    │
│  (Clustered Bar Chart)     │
└────────────────────────────┘
```

#### Right Column - Channel Performance
```
┌────────────────────────────┐
│  Sales by Channel          │
│  (Stacked Bar Chart)       │
│  - Online                  │
│  - Shop                    │
│  - Affiliate               │
│  - B2B                     │
└────────────────────────────┘
```

#### Bottom Row - Detailed Trend
```
┌─────────────────────────────────────────────────────┐
│  Daily Sales Trend (Line + Area Chart)              │
│  Last 90 Days with 7-Day & 30-Day Moving Avg       │
│  Tooltips: Date, Sales, Quantity, Orders           │
└─────────────────────────────────────────────────────┘
```

#### Matrix Table
```
┌─────────────────────────────────────────────────────┐
│  Sales by Company & Channel Matrix                  │
│  Rows: Company Source → Sales Channel               │
│  Values: Net Sales, Orders, AOV, Profit %          │
│  Conditional Formatting on Profit %                │
└─────────────────────────────────────────────────────┘
```

**Slicers**:
- Date Hierarchy (Year > Quarter > Month > Day)
- Sales Channel
- Transaction Type
- Company Source

---

## 📄 Page 3: Customer Analytics

### Layout: Customer-Centric View

#### Top Row - Customer KPIs (5 cards)
```
┌─────────────┬─────────────┬─────────────┬─────────────┬─────────────┐
│   Total     │    New      │   Repeat    │  Loyalty    │     CLV     │
│ Customers   │  Customers  │  Customers  │   Members   │             │
│   12,456    │    3,245    │    9,211    │    8,470    │  AED 2,048  │
│             │  26% of Tot │  74% of Tot │  68% Penetr │             │
└─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘
```

#### Middle Section - Customer Segmentation
```
┌────────────────────────────┬────────────────────────────┐
│  Customer Segmentation     │  Customer Acquisition      │
│  (Tree Map)                │  Trend (Area Chart)        │
│  By Loyalty Status         │  New vs Repeat Monthly     │
│  Verified vs Unverified    │                            │
└────────────────────────────┴────────────────────────────┘
```

#### Customer Behavior Analysis
```
┌────────────────────────────────────────────────────┐
│  Average Orders per Customer by Segment           │
│  (Clustered Column Chart)                         │
│  Segments: Loyalty Member, Verified, Channel      │
└────────────────────────────────────────────────────┘
```

#### Customer Distribution
```
┌──────────────────────┬──────────────────────────────┐
│  Top 20 Customers    │  Customer Distribution       │
│  by Sales            │  Histogram (Sales Bins)      │
│  (Bar Chart)         │  Show concentration          │
└──────────────────────┴──────────────────────────────┘
```

#### Detailed Customer Table
```
┌─────────────────────────────────────────────────────────┐
│  Customer Details Table                                 │
│  Columns: Customer Name, Phone, Total Sales,           │
│           Orders, AOV, Last Purchase Date, Status       │
│  Conditional Formatting: AOV, Frequency                │
│  Drill-through: Customer Transaction History           │
└─────────────────────────────────────────────────────────┘
```

**Slicers**:
- Customer Identity Status (Verified/Unverified)
- Loyalty Member (Yes/No)
- Sales Channel
- Location City
- First Purchase Date Range

---

## 📄 Page 4: Product Performance

### Layout: Product Analysis Grid

#### Top Row - Product KPIs
```
┌────────────┬────────────┬────────────┬────────────┬────────────┐
│ Active SKUs│ Categories │   Brands   │Avg Price/U │  Units Sold│
│   1,245    │     24     │    156     │  AED 125   │   204,567  │
└────────────┴────────────┴────────────┴────────────┴────────────┘
```

#### Product Performance Matrix
```
┌─────────────────────────────────────────────────────┐
│  Product Performance Scatter Chart                  │
│  X-Axis: Total Quantity Sold                       │
│  Y-Axis: Profit Margin %                           │
│  Bubble Size: Total Sales                          │
│  Color: ABC Classification                         │
│  Quadrant Lines: Average markers                   │
└─────────────────────────────────────────────────────┘
```

#### Category Performance
```
┌────────────────────────────┬────────────────────────────┐
│  Top 10 Categories         │  Category Contribution     │
│  by Sales (Bar Chart)      │  Waterfall Chart           │
│  With Growth % indicator   │  Top to Bottom             │
└────────────────────────────┴────────────────────────────┘
```

#### Brand & Division Analysis
```
┌────────────────────────────┬────────────────────────────┐
│  Sales by Division         │  Top Brands                │
│  (Donut Chart)             │  (Treemap)                 │
│  With Sort Order           │  Size by Sales             │
└────────────────────────────┴────────────────────────────┘
```

#### Product Details Table
```
┌──────────────────────────────────────────────────────────┐
│  Product Performance Table                               │
│  Columns: Item No, Item Name, Category, Brand,          │
│           Sales, Quantity, Avg Price, Profit %, Rank    │
│  Conditional Formatting: Profit %, Sales                │
│  Drill-through: Product Transaction Details             │
└──────────────────────────────────────────────────────────┘
```

#### ABC Analysis Chart
```
┌─────────────────────────────────────────────────────┐
│  Pareto Chart - Product Contribution                │
│  Bars: Individual Product Sales                     │
│  Line: Cumulative % Contribution                    │
│  Markers: 80% (A), 95% (B), 100% (C)               │
└─────────────────────────────────────────────────────┘
```

**Slicers**:
- Item Category (Hierarchy: Category > Subcategory)
- Division
- Brand
- Item Type
- ABC Classification

---

## 📄 Page 5: Location & Channel Analysis

### Layout: Geographical + Channel View

#### Location Performance Map
```
┌─────────────────────────────────────────────────────┐
│  UAE Map Visual (if available) or                   │
│  Filled Map by Location City                        │
│  Size: Sales, Color: Profit Margin %               │
│  Locations: Dubai, Abu Dhabi, Ras Al Khaimah       │
└─────────────────────────────────────────────────────┘
```

#### Top Locations Ranking
```
┌────────────────────────────┬────────────────────────────┐
│  Top 10 Locations          │  Location Performance      │
│  by Sales (Bar Chart)      │  Matrix                    │
│  With Rank & Contribution% │  City > Location Code      │
│                            │  Sales, Orders, AOV        │
└────────────────────────────┴────────────────────────────┘
```

#### Channel Analysis
```
┌─────────────────────────────────────────────────────┐
│  Sales Channel Breakdown (Stacked Column)           │
│  Over Time - Monthly                                │
│  Channels: Online, Shop, Affiliate, B2B, Service    │
└─────────────────────────────────────────────────────┘
```

#### Online vs Offline Deep Dive
```
┌────────────────────────────┬────────────────────────────┐
│  Online Channel Breakdown  │  Offline Store Performance │
│  (Pie Chart)               │  (Bar Chart)               │
│  Website, Android, iOS,    │  By Store Code             │
│  CRM, Unmapped             │  Sales & Foot Traffic      │
└────────────────────────────┴────────────────────────────┘
```

#### Channel Metrics Comparison
```
┌─────────────────────────────────────────────────────┐
│  Channel Comparison Table                           │
│  Rows: Sales Channel                                │
│  Metrics: Sales, Orders, AOV, Customers, Profit %  │
│  Conditional Formatting: All metrics               │
└─────────────────────────────────────────────────────┘
```

**Slicers**:
- Location City
- Location Code
- Sales Channel
- Online Order Channel (for online only)
- Offline Order Channel (for stores only)

---

## 📄 Page 6: Payment & Transaction Analysis

### Layout: Transaction-Focused

#### Transaction Overview KPIs
```
┌────────────┬────────────┬────────────┬────────────┬────────────┐
│Total Trans │   Sales    │  Refunds   │ Refund Rate│  Net Sales │
│   45,234   │ AED 26.8M  │ -AED 1.3M  │    4.8%    │ AED 25.5M  │
└────────────┴────────────┴────────────┴────────────┴────────────┘
```

#### Payment Method Analysis
```
┌────────────────────────────┬────────────────────────────┐
│  Payment Gateway Mix       │  Payment Method Split      │
│  (Donut Chart)             │  (Stacked Bar Chart)       │
│  Credit Card, Cash, COD,   │  Prepaid vs COD            │
│  Tabby, etc.               │  Over Time                 │
└────────────────────────────┴────────────────────────────┘
```

#### COD Analysis
```
┌─────────────────────────────────────────────────────┐
│  COD Rate by Channel & Location                     │
│  (Matrix Heatmap)                                   │
│  Shows COD preference by segment                    │
└─────────────────────────────────────────────────────┘
```

#### Transaction Type Breakdown
```
┌────────────────────────────┬────────────────────────────┐
│  Transaction Type Mix      │  Refund Trend              │
│  (Clustered Bar)           │  (Line Chart)              │
│  Sale, Refund, Other       │  Monthly Refund Rate %     │
└────────────────────────────┴────────────────────────────┘
```

#### Order Type Analysis (Online)
```
┌─────────────────────────────────────────────────────┐
│  Order Type Performance                             │
│  EXPRESS vs NORMAL vs EXCHANGE                      │
│  Metrics: Count, Sales, AOV, Avg Delivery Time      │
└─────────────────────────────────────────────────────┘
```

#### Payment Details Table
```
┌──────────────────────────────────────────────────────┐
│  Transaction Details Table                           │
│  Columns: Doc No, Date, Customer, Payment Method,   │
│           Transaction Type, Amount, Status           │
│  Drill-through: Transaction Line Items              │
└──────────────────────────────────────────────────────┘
```

**Slicers**:
- Transaction Type
- Payment Gateway
- Payment Method Code
- Order Type
- Sales Channel

---

## 📄 Page 7: Discount & Promotion Analysis

### Layout: Promotion Performance

#### Discount KPIs
```
┌────────────┬────────────┬────────────┬────────────┬────────────┐
│   Total    │  Discount  │ Discounted │   Avg      │  Online vs │
│  Discount  │    Rate    │   Orders   │  Discount  │  Offline   │
│ AED 2.7M   │   9.6%     │   18,234   │  AED 148   │  65% : 35% │
└────────────┴────────────┴────────────┴────────────┴────────────┘
```

#### Discount Trend Analysis
```
┌─────────────────────────────────────────────────────┐
│  Discount Rate Trend (Line Chart)                   │
│  Monthly - Shows discount % over time               │
│  Compare Online vs Offline discount rates          │
└─────────────────────────────────────────────────────┘
```

#### Discount Impact on Sales
```
┌────────────────────────────┬────────────────────────────┐
│  Sales by Discount Status  │  Discount Effectiveness    │
│  (Stacked Column)          │  (Scatter Plot)            │
│  Discounted vs No Discount │  X: Discount %, Y: Sales   │
└────────────────────────────┴────────────────────────────┘
```

#### Top Offers Performance
```
┌────────────────────────────┬────────────────────────────┐
│  Top Online Offers         │  Top Offline Offers        │
│  (Bar Chart)               │  (Bar Chart)               │
│  By redemption count       │  By redemption count       │
└────────────────────────────┴────────────────────────────┘
```

#### Discount Analysis by Segment
```
┌─────────────────────────────────────────────────────┐
│  Discount Penetration by Category                   │
│  (Clustered Bar Chart)                              │
│  Shows % of orders with discount per category       │
└─────────────────────────────────────────────────────┘
```

#### Offer Details Table
```
┌──────────────────────────────────────────────────────┐
│  Promotion Performance Table                         │
│  Columns: Offer Code, Offer Name, Channel,          │
│           Uses, Total Discount, Avg Discount, Sales  │
│  Sort by: Total Discount (Descending)               │
└──────────────────────────────────────────────────────┘
```

**Slicers**:
- Discount Status
- Has Discount (Yes/No)
- Online Offer No
- Offline Offer No
- Sales Channel
- Item Category

---

## 📄 Page 8: Time Trends & Forecasting

### Layout: Temporal Analysis

#### Period Comparison Cards
```
┌────────────┬────────────┬────────────┬────────────┐
│     MTD    │     YTD    │  Last Month│  Last Year │
│ AED 2.1M   │ AED 25.5M  │ AED 1.9M   │ AED 22.1M  │
│ ↑ 10.5%    │ ↑ 15.4%    │ AED 1.9M   │ AED 22.1M  │
└────────────┴────────────┴────────────┴────────────┘
```

#### Main Trend Line Chart
```
┌─────────────────────────────────────────────────────┐
│  Sales Trend - Last 24 Months                       │
│  Line Chart with Area Fill                         │
│  Lines: Current Year, Last Year                    │
│  Markers: Significant peaks/troughs                │
└─────────────────────────────────────────────────────┘
```

#### Seasonal Analysis
```
┌────────────────────────────┬────────────────────────────┐
│  Sales by Month (All Years)│  Sales by Day of Week      │
│  (Column Chart)            │  (Column Chart)            │
│  Shows seasonality pattern │  Mon-Sun pattern           │
└────────────────────────────┴────────────────────────────┘
```

#### Period Flags Utilization
```
┌─────────────────────────────────────────────────────┐
│  Dynamic Period Selector (Slicer + Measure)         │
│  Buttons: MTD | YTD | M-1 | M-2 | M-3 | Y-1 | Y-2  │
│  Display: Selected Period Sales                     │
└─────────────────────────────────────────────────────┘
```

#### Moving Averages
```
┌─────────────────────────────────────────────────────┐
│  Sales with Moving Averages                         │
│  Lines: Daily Sales, 7-Day MA, 30-Day MA           │
│  Helps identify trends and smooth volatility       │
└─────────────────────────────────────────────────────┘
```

#### Year-over-Year Comparison Matrix
```
┌─────────────────────────────────────────────────────┐
│  YoY Comparison Table                               │
│  Rows: Month                                        │
│  Columns: 2023, 2024, 2025, Growth %               │
│  Conditional Formatting: Growth %                  │
└─────────────────────────────────────────────────────┘
```

#### Simple Forecast (if time series is stable)
```
┌─────────────────────────────────────────────────────┐
│  Sales Forecast - Next 3 Months                     │
│  Line Chart with Forecast (built-in PBI forecast)   │
│  Confidence interval: 95%                           │
└─────────────────────────────────────────────────────┘
```

**Slicers**:
- Date Range Picker
- Year
- Quarter
- Month
- Dynamic Period Selector (Custom)

---

## 🎨 Design Guidelines

### Color Scheme
```
Primary Palette (Pet Shop Brand):
- Primary Blue: #0078D4
- Success Green: #107C10
- Warning Orange: #FF8C00
- Alert Red: #D13438
- Neutral Gray: #605E5C

Secondary Palette:
- Light Blue: #DEECF9
- Light Green: #DFF6DD
- Light Orange: #FFF4CE
- Light Red: #FDE7E9
- Background: #FAFAFA
```

### Typography
- **Headers**: Segoe UI, Bold, 16-20pt
- **KPIs**: Segoe UI, Semibold, 28-36pt
- **Body Text**: Segoe UI, Regular, 11pt
- **Labels**: Segoe UI, Regular, 9-10pt

### Visual Best Practices
1. **Consistency**: Use same colors for same metrics across pages
2. **White Space**: Don't overcrowd - use 10-15px margins
3. **Tooltips**: Rich tooltips with multiple metrics and context
4. **Drill-through**: Enable on all detail tables
5. **Bookmarks**: Create for common filter combinations
6. **Mobile Layout**: Create mobile-optimized layouts for each page

### Interactive Elements
1. **Cross-filtering**: Enable between related visuals
2. **Drill-down**: Enable on hierarchies (Date, Location, Product)
3. **Sync Slicers**: Sync date and key dimensions across pages
4. **Buttons**:
   - Clear Filters button on each page
   - Navigate between pages
   - Toggle between views (e.g., Chart vs Table)
5. **Bookmarks**:
   - Save default view
   - Common analysis scenarios
   - Filtered views for different user roles

---

## 📱 Mobile Layout Considerations

Create mobile-optimized layouts with:
- Portrait orientation (9:16)
- Larger touch targets (minimum 44x44 pixels)
- Simplified visuals (max 2-3 per screen)
- Essential KPIs only
- Swipe navigation between pages

---

## 🔐 Row-Level Security (RLS)

Implement RLS for:
```dax
// Location-based security
[location_code] = USERPRINCIPALNAME()

// Manager-level security
[user_id] = USERPRINCIPALNAME()

// Company-level security
[company_source] IN VALUES(UserAccess[AllowedCompanies])
```

---

## ⚡ Performance Optimization

1. **Aggregations**: Create aggregation tables for large datasets
2. **DirectQuery**: Use for real-time data if needed
3. **Incremental Refresh**: Set up for fact_commercial table
4. **Reduce Cardinality**: Use ID fields instead of text where possible
5. **Optimize DAX**: Use variables, avoid complex calculated columns
6. **Limit Visuals**: Max 15-20 visuals per page

---

## 📊 Additional Features

### Field Parameters for Dynamic Analysis
```dax
// Create parameter table for measure selection
Sales Metrics = {
    ("Net Sales", NAMEOF('Measures'[Total Net Sales]), 0),
    ("Gross Sales", NAMEOF('Measures'[Total Gross Sales]), 1),
    ("Profit", NAMEOF('Measures'[Gross Profit]), 2),
    ("Quantity", NAMEOF('Measures'[Total Quantity Sold]), 3),
    ("Orders", NAMEOF('Measures'[Total Orders]), 4)
}
```

### Smart Narratives
Add AI-powered insights on each page using Power BI's Smart Narrative visual.

### Key Influencers
Add Key Influencers visual to identify:
- What drives high sales?
- What impacts customer loyalty?
- What influences profit margins?

### Decomposition Tree
Allow users to drill into:
- Sales by Channel → Location → Product
- Profit by Division → Category → Brand

---

## 🚀 Implementation Checklist

- [ ] Import fact_commercial data from BigQuery/Data source
- [ ] Create all DAX measures from DAX_Measures.md
- [ ] Set up date table with fiscal calendar
- [ ] Configure relationships in data model
- [ ] Create measure groups/folders for organization
- [ ] Build Page 1: Executive Summary
- [ ] Build Page 2: Sales Performance
- [ ] Build Page 3: Customer Analytics
- [ ] Build Page 4: Product Performance
- [ ] Build Page 5: Location & Channel
- [ ] Build Page 6: Payment & Transactions
- [ ] Build Page 7: Discount Analysis
- [ ] Build Page 8: Time Trends
- [ ] Configure cross-filtering and drill-through
- [ ] Create bookmarks for common views
- [ ] Design mobile layouts
- [ ] Set up RLS (if required)
- [ ] Configure incremental refresh
- [ ] Test performance and optimize
- [ ] Create user documentation
- [ ] Deploy to Power BI Service
- [ ] Set up scheduled refresh
- [ ] Share with stakeholders

---

**Created by**: Claude - Power BI Developer
**Last Updated**: 2025-11-08
**Version**: 1.0
