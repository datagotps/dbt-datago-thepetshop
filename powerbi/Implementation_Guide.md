# 🚀 Power BI Dashboard Implementation Guide
## The Pet Shop Commercial Analytics - Step-by-Step Setup

---

## 📋 Table of Contents
1. [Prerequisites](#prerequisites)
2. [Data Connection Setup](#data-connection-setup)
3. [Data Model Configuration](#data-model-configuration)
4. [DAX Measures Implementation](#dax-measures-implementation)
5. [Dashboard Creation](#dashboard-creation)
6. [Publishing & Sharing](#publishing--sharing)
7. [Maintenance & Updates](#maintenance--updates)

---

## 1. Prerequisites

### Required Software
- **Power BI Desktop** (Latest version)
  - Download from: https://powerbi.microsoft.com/desktop
  - Recommended: Monthly update channel

### Required Access
- Access to BigQuery project containing `fact_commercial` table
- Google Cloud Service Account credentials (if using service account)
- Or Google user credentials with read access to the dataset

### Skills Required
- Basic Power BI knowledge
- Understanding of DAX (provided in this guide)
- Familiarity with data modeling concepts

---

## 2. Data Connection Setup

### Option A: Connect to BigQuery

#### Step 1: Open Power BI Desktop
1. Launch Power BI Desktop
2. Click **Get Data** → **More...**
3. Search for "Google BigQuery"
4. Click **Connect**

#### Step 2: Configure BigQuery Connection
```
Billing Project ID: [Your GCP Project ID]
Use Storage API: ✓ (for better performance)
```

#### Step 3: Authenticate
1. Choose **OAuth 2.0** authentication
2. Sign in with your Google account
3. Grant necessary permissions

#### Step 4: Navigate to fact_commercial
```
Project → Dataset → Tables → fact_commercial
```

#### Step 5: Load Options
**Choose Import Mode** (Recommended for best performance)
- Faster report rendering
- Better user experience
- Requires scheduled refresh

**Or DirectQuery** (For real-time data)
- Always shows latest data
- Slower performance
- No refresh needed

**Recommended Settings:**
- **Mode**: Import
- **Storage Format**: Columnar (automatic)
- **Incremental Refresh**: Configure after initial load

### Option B: Connect to CSV/Excel (For Testing)

If you want to test with sample data:
1. Export a sample from BigQuery
2. **Get Data** → **Text/CSV** or **Excel**
3. Select your file and load

---

## 3. Data Model Configuration

### Step 1: Create Date Table

This is crucial for time intelligence functions.

#### Method 1: DAX Calculated Table
```dax
Date Table =
ADDCOLUMNS(
    CALENDAR(
        DATE(2020, 1, 1),  -- Start date
        DATE(2026, 12, 31)  -- End date
    ),
    "Year", YEAR([Date]),
    "Year Month", FORMAT([Date], "YYYY-MM"),
    "Year Month Number", YEAR([Date]) * 100 + MONTH([Date]),
    "Quarter", "Q" & FORMAT([Date], "Q"),
    "Quarter Number", QUARTER([Date]),
    "Month", FORMAT([Date], "MMMM"),
    "Month Number", MONTH([Date]),
    "Month Short", FORMAT([Date], "MMM"),
    "Week Number", WEEKNUM([Date]),
    "Week Day", FORMAT([Date], "dddd"),
    "Week Day Number", WEEKDAY([Date]),
    "Day", DAY([Date]),
    "Is Weekend", IF(WEEKDAY([Date]) IN {1, 7}, TRUE(), FALSE()),
    "Fiscal Year", IF(MONTH([Date]) >= 4, YEAR([Date]), YEAR([Date]) - 1), -- Adjust based on fiscal year
    "Fiscal Quarter",
        SWITCH(
            MONTH([Date]),
            1, "Q3", 2, "Q3", 3, "Q3",
            4, "Q4", 5, "Q4", 6, "Q4",
            7, "Q1", 8, "Q1", 9, "Q1",
            10, "Q2", 11, "Q2", 12, "Q2"
        )
)
```

**Create the table:**
1. Go to **Modeling** tab
2. Click **New Table**
3. Paste the DAX code above
4. Press Enter

#### Mark as Date Table
1. Right-click on "Date Table" in Fields pane
2. Select **Mark as date table**
3. Choose **Date** column
4. Click OK

### Step 2: Create Relationships

#### Relationship 1: fact_commercial → Date Table
```
fact_commercial[posting_date] → 'Date Table'[Date]
Cardinality: Many to One (*)→(1)
Cross filter direction: Single
Make this relationship active: Yes
```

**To create:**
1. Go to **Model** view
2. Drag `posting_date` from fact_commercial to `Date` in Date Table
3. Verify relationship settings
4. Click OK

### Step 3: Create Helper Tables (Optional)

#### Measure Selector Table (for dynamic visuals)
```dax
Measure Selector = {
    "Net Sales",
    "Gross Sales",
    "Profit",
    "Quantity",
    "Orders"
}
```

#### Period Selector Table
```dax
Period Selector = {
    "MTD",
    "YTD",
    "Last Month",
    "Last Year",
    "Custom"
}
```

**Note:** Mark these as "Do not summarize" and hide from report view.

### Step 4: Organize Tables

1. **Create Display Folders** in fact_commercial:
   - Right-click fields → Properties → Display Folder
   - Group related fields:
     - `Financial Metrics` (sales, cost, discount fields)
     - `Customer Info` (customer name, phone, etc.)
     - `Product Info` (item fields)
     - `Location` (location fields)
     - `Identifiers` (IDs, document numbers)

2. **Hide Unnecessary Fields**:
   - Technical IDs that won't be used in reports
   - Right-click → Hide in report view

---

## 4. DAX Measures Implementation

### Step 1: Create Measures Table

**Best Practice:** Store all measures in a dedicated table
```dax
_Measures = ROW("Measures Table", 1)
```

1. **Modeling** → **New Table**
2. Paste code above
3. Hide the "Measures Table" column
4. This table will hold all your measures

### Step 2: Organize Measures into Folders

Create measures organized by category:

#### Create a Measure
1. Select `_Measures` table
2. **Modeling** → **New Measure**
3. Type the measure formula
4. Set format (Currency, Percentage, Whole Number, etc.)
5. Set display folder

#### Example: Creating Total Net Sales
```dax
Total Net Sales = SUM(fact_commercial[sales_amount__actual_])
```

**Steps:**
1. Click **New Measure**
2. Paste formula
3. In Properties:
   - Format: Currency
   - Currency: AED (or appropriate)
   - Decimal Places: 2
   - Display Folder: "1. Core Sales Metrics"

### Step 3: Implement All Measures from DAX_Measures.md

Copy all measures from `DAX_Measures.md` file and create them in Power BI.

**Recommended Order:**
1. Core Sales Metrics (essential)
2. Time Intelligence (critical for trends)
3. Customer Analytics
4. Product Performance
5. Discount Analysis
6. Advanced measures

**Keyboard Shortcuts:**
- `Ctrl + Shift + M`: New measure
- `Ctrl + C` / `Ctrl + V`: Copy/paste DAX code
- `Tab`: Autocomplete

### Step 4: Validate Measures

Test each measure:
1. Create a **Card** visual
2. Add the measure
3. Verify the result makes sense
4. Check formatting
5. Delete test visual

**Common Issues:**
- **Blank results**: Check if relationship is active
- **Unexpected values**: Verify filter context
- **Errors**: Check column names and syntax

---

## 5. Dashboard Creation

### Page 1: Executive Summary

#### Step 1: Set Page Size & Background
1. **View** → **Page View** → Select page
2. **Format** → **Canvas Settings**
   - Type: Custom (1920 x 1080) or 16:9
   - Background Color: #FAFAFA

#### Step 2: Add KPI Cards

**Card Visual 1: Total Net Sales**
1. **Visualizations** → **Card**
2. Add `Total Net Sales` measure
3. Format:
   - **Callout Value**:
     - Font: Segoe UI, 36pt, Bold
     - Color: #0078D4
   - **Category Label**: "Total Net Sales"
     - Font: Segoe UI, 12pt, Regular
     - Color: #605E5C
   - **Effects** → **Background**: White
   - **Border**: 1px, Light gray
   - **Padding**: 10px

**Repeat for other KPIs:**
- Gross Profit
- Total Orders
- Total Customers

**Add YoY Growth Indicator:**
1. Add a **Text box** below each KPI
2. Use conditional formatting or manually enter: "↑ 15.3% YoY"
3. Color: Green for positive, Red for negative

#### Step 3: Add Performance Gauges

**Gauge Visual: Sales Target**
1. **Visualizations** → **Gauge**
2. **Value**: `Total Net Sales`
3. **Target**: `Sales Target`
4. **Maximum**: Set to target * 1.2
5. Format:
   - **Gauge Axis**: Max = Target * 1.2
   - **Colors**:
     - Red: 0-75%
     - Yellow: 75-100%
     - Green: 100%+

#### Step 4: Add Sales Trend Line Chart

1. **Visualizations** → **Line Chart**
2. **X-axis**: `Date Table[Date]` (or `Year Month`)
3. **Y-axis**: `Total Net Sales`
4. Add second line: `30-Day Moving Avg`
5. Format:
   - **Legend**: Bottom
   - **Data Labels**: Off (for cleaner look)
   - **Line Styles**:
     - Sales: Solid, 3px, #0078D4
     - Moving Avg: Dashed, 2px, #FF8C00
   - **Title**: "Sales Trend - Last 12 Months"

#### Step 5: Add Channel Mix Donut Chart

1. **Visualizations** → **Donut Chart**
2. **Legend**: `sales_channel`
3. **Values**: `Total Net Sales`
4. Format:
   - **Detail Labels**: Show, Category + Percentage
   - **Colors**: Custom for each channel
   - **Title**: "Sales by Channel"

#### Step 6: Add Top Performers Bar Charts

**Top 5 Products:**
1. **Visualizations** → **Bar Chart** (horizontal)
2. **Axis**: `item_name`
3. **Values**: `Total Net Sales`
4. **Filters**:
   - Add `Product Rank` to Visual filters
   - Set to: "is less than or equal to 5"
5. Sort: Descending by Total Net Sales

**Repeat for:**
- Top 5 Locations (use `location_code`)
- Top 5 Categories (use `item_category`)

#### Step 7: Add Slicers (Right Panel)

**Date Range Slicer:**
1. **Visualizations** → **Slicer**
2. Field: `Date Table[Date]`
3. Slicer Settings:
   - Style: Between
   - Responsive layout
4. Format:
   - Background: Light blue (#DEECF9)
   - Border: 1px
   - Padding: 10px

**Other Slicers:**
- `company_source`: Dropdown or Tile
- `sales_channel`: Tile style
- `location_city`: Dropdown
- `transaction_type`: Tile

**Slicer Panel:**
1. Group all slicers in right panel (300px width)
2. Align vertically with 10px spacing
3. Add panel background rectangle

#### Step 8: Add Navigation & Interactivity

**Clear Filters Button:**
1. **Insert** → **Buttons** → **Blank**
2. Text: "Clear All Filters"
3. Action: **Bookmark** → Link to bookmark with no filters
4. Style: Rectangle, #D13438 background, White text

**Create Bookmark:**
1. **View** → **Bookmarks**
2. **Add** → Name: "Default View"
3. Update this bookmark with no filters applied

### Pages 2-8: Follow Dashboard_Design.md

Repeat similar process for other pages using the layouts in `Dashboard_Design.md`.

**Key Tips:**
- **Copy/Paste**: Copy visuals from Page 1 to maintain consistency
- **Alignment**: Use **Format** → **Align** tools
- **Snap to Grid**: Enable for precise placement
- **Groups**: Group related visuals together
- **Sync Slicers**: Use **View** → **Sync Slicers** for date and key filters

---

## 6. Publishing & Sharing

### Step 1: Save Your Work
```
File → Save As
Location: Choose location
Name: ThePetShop_Commercial_Dashboard.pbix
```

### Step 2: Publish to Power BI Service

1. **Home** → **Publish**
2. Sign in to Power BI Service
3. Select destination workspace
   - Create new workspace if needed: "The Pet Shop Analytics"
4. Click **Select**
5. Wait for upload to complete
6. Click **Open in Power BI**

### Step 3: Configure Scheduled Refresh

**In Power BI Service:**
1. Go to workspace
2. Find your dataset (not the report)
3. Click **⋮** → **Settings**
4. **Data source credentials**:
   - Click **Edit credentials**
   - Authentication: OAuth2
   - Sign in with BigQuery account
5. **Scheduled refresh**:
   - Toggle ON
   - Frequency: Daily
   - Time: Choose low-traffic time (e.g., 6:00 AM)
   - Time zone: Select appropriate timezone
   - Send failure notification: Enable
   - Email: Your email
6. **Refresh history**: Monitor for issues

### Step 4: Set Up Incremental Refresh (Optional)

**For large datasets (millions of rows):**

**In Power BI Desktop:**
1. Right-click `fact_commercial` table
2. **Incremental refresh**
3. Configure:
   ```
   Archive data starting: 2 years before refresh date
   Incrementally refresh data starting: 7 days before refresh date
   Detect data changes: posting_date
   ```
4. Click OK
5. Republish

### Step 5: Share Dashboard

**Option A: Share with Specific Users**
1. Open report in Power BI Service
2. Click **Share**
3. Enter email addresses
4. Permissions:
   - ☑ Allow recipients to share
   - ☑ Allow recipients to build content
   - ☐ Send email notification (optional)
5. Click **Share**

**Option B: Create App**
1. Go to workspace
2. **Create app**
3. Setup:
   - Name: "The Pet Shop Commercial Analytics"
   - Description: "Comprehensive sales & customer analytics"
   - Logo: Upload company logo
4. **Navigation**: Arrange report pages
5. **Permissions**: Add users or groups
6. **Publish app**

**Option C: Embed in Website/Teams**
1. Open report
2. **File** → **Embed** → **Website or portal**
3. Copy embed code
4. Paste in your website/SharePoint

### Step 6: Set Up Row-Level Security (If Needed)

**In Power BI Desktop:**
1. **Modeling** → **Manage Roles**
2. **Create** role: "Location Manager"
3. Add filter to `fact_commercial`:
   ```dax
   [location_code] = USERPRINCIPALNAME()
   ```
4. Click **Save**
5. Republish

**In Power BI Service:**
1. Go to dataset → **Security**
2. Select role
3. Add members (emails)
4. Click **Save**

---

## 7. Maintenance & Updates

### Regular Maintenance Tasks

#### Daily
- ✓ Check scheduled refresh status
- ✓ Monitor report usage (if needed)

#### Weekly
- ✓ Review report performance
- ✓ Check for new requirements from users
- ✓ Validate data accuracy (spot checks)

#### Monthly
- ✓ Update DAX measures (if needed)
- ✓ Add new visuals based on feedback
- ✓ Review and optimize slow visuals
- ✓ Update documentation

#### Quarterly
- ✓ Review and optimize data model
- ✓ Archive old data (if using incremental refresh)
- ✓ Update targets and benchmarks
- ✓ Train new users

### Performance Optimization

**If dashboard is slow:**

1. **Check Data Volume**
   ```
   Total Rows = COUNTROWS(fact_commercial)
   ```
   If > 10 million rows, consider:
   - Incremental refresh
   - Aggregations
   - DirectQuery for historical data

2. **Optimize DAX**
   - Use variables
   - Avoid calculated columns (use measures)
   - Remove unused measures
   - Use TREATAS instead of FILTER when possible

3. **Reduce Visual Complexity**
   - Limit to 15-20 visuals per page
   - Remove unnecessary visuals
   - Use drill-through instead of detail on main page

4. **Use Performance Analyzer**
   - **View** → **Performance Analyzer**
   - **Start Recording**
   - Interact with report
   - **Stop Recording**
   - Analyze DAX query times
   - Optimize slow measures

### Updating the Dashboard

**To add new measures:**
1. Open `.pbix` file
2. Create new measure
3. Add to appropriate visual
4. Test thoroughly
5. Publish to service

**To modify visuals:**
1. Edit in Desktop
2. Test with filters and interactions
3. Publish (overwrites existing)
4. Refresh browser in Service

**Version Control:**
- Save dated versions: `Dashboard_v1.0_2025-01.pbix`
- Keep backup before major changes
- Document changes in changelog

---

## 8. Troubleshooting

### Common Issues

#### Issue: "Data source credentials needed"
**Solution:**
1. Power BI Service → Dataset Settings
2. Data source credentials → Edit
3. Re-authenticate

#### Issue: Refresh failing
**Solution:**
1. Check refresh history for error message
2. Common causes:
   - Credentials expired: Re-authenticate
   - Query timeout: Reduce data range or use incremental refresh
   - Schema change: Republish from Desktop
   - BigQuery quota: Check GCP quotas

#### Issue: Visuals showing wrong data
**Solution:**
1. Check filters applied
2. Verify relationships are active
3. Check measure DAX logic
4. Clear cache: Ctrl + F5 in browser

#### Issue: Slow performance
**Solution:**
1. Use Performance Analyzer
2. Reduce number of visuals
3. Optimize DAX (use variables)
4. Enable query reduction
5. Consider aggregations

---

## 9. Best Practices Checklist

### Data Model
- ☑ Date table created and marked
- ☑ Relationships properly configured
- ☑ Unnecessary fields hidden
- ☑ Fields organized in display folders
- ☑ Correct data types assigned

### DAX Measures
- ☑ All measures in dedicated table
- ☑ Organized in display folders
- ☑ Properly formatted (currency, %, etc.)
- ☑ Clear, descriptive names
- ☑ Comments for complex logic

### Visualizations
- ☑ Consistent color scheme
- ☑ Proper titles on all visuals
- ☑ Appropriate visual types
- ☑ Tooltips configured
- ☑ Drill-through enabled where needed
- ☑ Mobile layout created

### Performance
- ☑ < 15-20 visuals per page
- ☑ Incremental refresh configured
- ☑ Query reduction enabled
- ☑ Unused fields removed
- ☑ Calculated columns minimized

### Publishing
- ☑ Scheduled refresh configured
- ☑ Credentials properly set
- ☑ Sharing configured
- ☑ RLS implemented (if needed)
- ☑ Documentation provided to users

---

## 10. Resources

### Power BI Resources
- **Official Documentation**: https://docs.microsoft.com/power-bi/
- **DAX Guide**: https://dax.guide/
- **Power BI Community**: https://community.powerbi.com/
- **YouTube Channel**: Guy in a Cube

### Support
- **Power BI Support**: https://powerbi.microsoft.com/support/
- **BigQuery Documentation**: https://cloud.google.com/bigquery/docs

### Training
- **Microsoft Learn**: Power BI learning paths
- **SQLBI**: Advanced DAX courses
- **Enterprise DNA**: Power BI tutorials

---

## 📞 Need Help?

If you encounter issues:
1. Check Troubleshooting section above
2. Review Power BI Community forums
3. Contact your Power BI admin
4. Reach out to the data team

---

**Document Version**: 1.0
**Last Updated**: 2025-11-08
**Created by**: Claude - Power BI Developer & Data Visualization Expert
**Maintained by**: [Your Team Name]
