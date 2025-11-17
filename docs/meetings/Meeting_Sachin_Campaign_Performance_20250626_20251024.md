# Meeting Segmentation Analysis: Campaign Performance Dashboard Project

**Project:** Campaign Performance Reporting Automation  
**Stakeholder:** Sachin (Campaign Manager)  
**Analyst:** Anmar Abbas (BI Consultant)  
**Client:** The Petshop  
**Meeting Dates:** June 26, 2025 & October 24, 2025  
**Total Duration:** ~133 minutes combined

---

## Executive Summary

Two critical discovery sessions to automate campaign performance reporting at The Petshop. Currently, Sachin spends 5 hours/week manually compiling reports from multiple data sources (ERP, Power BI, Shopify). The project aims to create automated dashboards showing campaign contribution to business and individual campaign performance. Key challenges include baseline calculation methodology, discount code mapping across systems, and Shopify line-item level discount tracking limitations.

**Critical Outcome:** Identified data sources, documented manual process, discovered significant technical gaps in online discount tracking, and planned Slim Stock integration for improved baseline forecasting.

---

## Meeting Segments - Session 1 (June 26, 2025)

| Time Range | Duration | Topic/Concept | Key Discussion Points | Decisions/Actions |
|------------|----------|---------------|----------------------|-------------------|
| 0:00-1:53 | 1.9 min | Introduction & Session Objectives | • Anmar heard good things about Sachin's analytical skills<br>• Session goal: Understand numbers Sachin tracks<br>• Show current manual process<br>• Identify dashboard requirements | • Sachin to walk through weekly report<br>• Focus on backward compatibility approach |
| 1:53-7:09 | 5.3 min | Revenue Classification Framework | • **Total revenue breakdown**: 3.3M for December with 1.1M margin<br>• ~50% revenue from organic (no discount), 50% from promotions<br>• **Code-based discounts**: Loyalty programs (ESAAD, FAZAA) using store codes<br>• **Thematic campaigns**: Direct price-offs on specific products<br>• **Spend & Save**: Threshold promotions (e.g., spend 300 get 20% off)<br>• **Mechanics**: Category-level (e.g., buy 2 get more, 20% off)<br>• **Brand campaigns**: Flat discounts on entire brands<br>• **Clearance**: End-of-life inventory | • System auto-applies highest discount when campaigns overlap<br>• All classification tracked for both online and offline |
| 7:09-10:04 | 3.0 min | Campaign Performance Metrics | • **Campaign performance report** tracks revenue vs targets<br>• **Uplift calculation**: Baseline vs campaign performance<br>• Uplift methodology acknowledged as "questionable"<br>• Example: Product normally sells 5 units, with promotion sells 8 units = 3 units uplift | • Need proper baseline methodology<br>• Current approach is "gray area" not black/white |
| 10:04-13:19 | 3.3 min | Baseline Calculation Challenge | • Baselines change frequently due to constant campaigns<br>• **Current method**: Look at BI report for similar week from previous month<br>• Uses **Slim Stock** for demand forecasting (quantity-level)<br>• Slim Stock doesn't map to revenue/margin - only quantities | • Two main reports identified as priority<br>• Campaign summary and performance reports most critical |
| 13:19-21:02 | 7.7 min | Promotion Planning Process | • **Weekly pattern**: Last week + first week of month are strongest (payday effect)<br>• Planning example: July 1-7 campaign for Purina brand at 15% off<br>• **Baseline simulation**: Match day-of-week patterns from previous months<br>• Question raised: Why January baseline vs. July last year?<br>• Answer: Year-over-year baselines outdated due to growth (May: 3.4M → 3.9M YoY) | • Always use recent month data for baseline<br>• Match day-of-week patterns, not calendar dates |
| 21:02-30:19 | 9.3 min | Slim Stock Integration Discussion | • Slim Stock has 3-month forward forecasts<br>• Data currently only in quantity, not revenue/margin<br>• **Solution proposed**: Flow Slim Stock data to BI with RSP and margin mapping<br>• If connected properly, could predict "if we don't discount, this is June revenue"<br>• **Dashboard concept**: Filter by date range, get automatic baseline for planning | • Anmar to investigate if Slim Stock data can flow to BI<br>• Need to map quantities to revenue using standard pricing |
| 30:19-38:28 | 8.2 min | Data Source Walkthrough - Stores | • **Three input sources identified**:<br>&nbsp;&nbsp;- ERP: Discount Ledger<br>&nbsp;&nbsp;- ERP: Item Ledger<br>&nbsp;&nbsp;- BI: Sales reports<br>• ERP uses **document date** not posting date<br>• Document date = when customer placed order (relevant for campaign attribution)<br>• Posting date = when revenue realized (delivery date) | • Sachin to provide December 2024 files as case study<br>• Use document date for both online and offline consistency |
| 38:28-43:09 | 4.7 min | Shopify Data Requirements | • **Critical limitation**: BI doesn't have promo code information<br>• Must export from Shopify to get discount codes<br>• Shopify export contains order-level promo codes<br>• "Tricky part" mentioned for later discussion | • Need to bring Shopify promo code data to BI<br>• Discount ledger for offline, Shopify for online<br>• Files to be provided for December 2024 |
| 43:09-End | Remaining | Next Steps & Baseline Strategy | • Agreement to tackle baseline calculation separately<br>• Customer Dashboard currently in progress<br>• DSR (Daily Sales Report) next priority<br>• Commercial reporting suite will include campaign analysis<br>• **Time saved**: Currently 5 hours/week on manual reporting<br>• CEO approach example: Weekly pockets, compare vs. last week and same week last year | • Anmar to receive input files<br>• Calendar invite for practical walkthrough scheduled<br>• Slim Stock baseline calculation to be explored<br>• Focus on modernizing DSR first, then campaign reports |

---

## Meeting Segments - Session 2 (October 24, 2025)

| Time Range | Duration | Topic/Concept | Key Discussion Points | Decisions/Actions |
|------------|----------|---------------|----------------------|-------------------|
| 0:00-3:45 | 3.8 min | Session Kickoff & Report Review | • Second session on campaign reporting<br>• Review of May campaign report emailed previously<br>• Two main reports needed: **Campaign Summary** and **Campaign Performance**<br>• Campaign Performance treated as "stage two" (more complex) | • Focus on Campaign Summary as priority<br>• Stage two to be addressed later |
| 3:45-7:01 | 3.3 min | Campaign Summary Structure | • Categories revenue into: Codes, Thematic Campaigns, Clearance, Organic<br>• **Codes**: ESAAD, FAZAA, Anmar Platinum loyalty programs<br>• Each campaign shows: Revenue, Margin, Discount given<br>• Online and offline both tracked separately<br>• Online is "more problematic" than stores for data | • Online requires workarounds due to Shopify limitations<br>• Stores data "straightforward" from ERP |
| 7:01-14:04 | 7.1 min | ERP Discount Ledger Deep Dive | • **Discount Ledger** contains: Document number, Discount type, Offer number<br>• Example examined: Document Anmar1414 with discount<br>• **Offer Number** is the campaign code/promotion code<br>• Data available in BI data model already (discount entry table)<br>• Sales amount, discount amount, entry number all present | • Offer Number field critical for campaign mapping<br>• Entry number links discount ledger to item ledger |
| 14:04-23:29 | 9.4 min | Offer Name Mapping Challenge | • Offer Number (e.g., "82") needs mapping to **Offer Name** (human-readable)<br>• Mapping table exists in ERP: **Periodic Discount Offers**<br>• Example: Store Code E-82 = "Affiliate 50"<br>• Description field contains the offer name<br>• Need to bring this "Periodic Discount" table into BI<br>• **Offer Type** (numeric code) also exists but meaning unclear | • Screenshot taken of Periodic Discount table structure<br>• Task created: Get offer type code definitions from IT<br>• Need to stitch Periodic Discount description to campaign data |
| 23:29-30:01 | 6.5 min | Data Model Discovery - Tables | • **Tables identified**:<br>&nbsp;&nbsp;- Discount Ledger Entry (has offer number, entry number, amounts)<br>&nbsp;&nbsp;- Trans Discount Entry (alternative table)<br>&nbsp;&nbsp;- Periodic Discount (has offer number, description)<br>&nbsp;&nbsp;- Periodic Discount Line (child table, hierarchy unclear)<br>• Entry Number is primary key linking orders to discounts<br>• Document + Item Number combination needed for line-level accuracy | • Entry number matching critical (one order can have partial discounts)<br>• Anmar to check if Periodic Discount accessible in data warehouse |
| 30:01-33:03 | 3.0 min | Campaign Classification Logic | • **Classification types**: Codes vs. Thematic vs. Brand vs. Mechanics<br>• Currently NO classification in ERP system<br>• Sachin **manually classifies** based on offer name/description<br>• Solution: Create mapping table with "if-then" logic | • Manual classification acceptable for now<br>• Create Excel mapping: offer name → campaign type<br>• Can be automated via lookup table |
| 33:03-42:01 | 9.0 min | Shopify Discount Code Challenge | • Shopify has discount codes at **order level**, not line-item level<br>• Example order examined: Multiple items, one discount code<br>• **Critical limitation**: Cannot determine which line items got discount<br>• Partial discounts within orders invisible<br>• Shopify applies discount to entire cart | • Accept order-level discount limitation for now<br>• Need Shopify data sync to BI via API<br>• Explore if line-item discount data exists anywhere in Shopify |
| 42:01-50:10 | 8.2 min | Discount Code Location in Shopify | • Found discount code in Shopify data: `orders.discount_code.code`<br>• Exists at order level only<br>• Multiple discount codes possible per order (e.g., "PET15" + another)<br>• **Test case requested**: Order with partial line items discounted<br>• Need to verify if line-level data exists anywhere | • Anmar to test with order having partial discounts<br>• Check if order line level contains discount attribution<br>• Sarah to provide Shopify data sync to BI |
| 50:10-59:00 | 8.8 min | Offer Type Code Mapping | • **Offer Type** numeric codes discovered:<br>&nbsp;&nbsp;- 0 = Promotion<br>&nbsp;&nbsp;- 1 = Deal<br>&nbsp;&nbsp;- 2 = Multibuy<br>&nbsp;&nbsp;- 3 = Mix & Match<br>&nbsp;&nbsp;- 4 = Total Discount<br>• SQL stores codes as numbers, application shows text labels<br>• Screenshot taken for documentation<br>• All dropdowns in ERP work this way (code in database, label in UI) | • Create CASE statement to map codes to labels<br>• Offer Type useful for classification<br>• Screenshot documented for reference |
| 59:00-1:07:00 | 8.0 min | Online Discount Limitations Discussion | • Shopify only tracks promo code discounts<br>• **Direct price discounts** (thematic campaigns) NOT tracked in Shopify<br>• Example: If SKU price changed from 500 to 450, Shopify shows 450 (no discount code)<br>• **Manual workaround**: Sachin maps SKU sold during campaign dates → campaign revenue<br>• Cannot do campaign analysis automatically for price-off promotions | • Accept that online thematic campaigns require manual mapping<br>• System limitation acknowledged<br>• Focus on code-based campaigns for automation |
| 1:07:00-1:19:26 | 12.4 min | Multiple Discounts Scenario | • Question: Can one order have items with different discounts?<br>• Example found: Order with "PET20" code + item on price-off<br>• Customers **CANNOT combine two promo codes**<br>• But CAN combine: Price-off item + promo code on order<br>• Price-off doesn't show as discount in Shopify (just lower price)<br>• Only promo code tracked | • Order-level discount code is what's trackable<br>• Line-level partial discounts not in Shopify data<br>• Focus on promo code campaigns for online automation |
| 1:19:26-1:27:44 | 8.3 min | Data Mapping & File Structure | • Reviewed Periodic Discount table structure<br>• Confirmed NO document number in Periodic Discount (it's a master table)<br>• Must join: Discount Entry (has entry number + offer number) → Periodic Discount (has offer number + description)<br>• Successfully tested: Found "Affiliate 50" description for offer number<br>• Mapping logic validated | • Join tables via Offer Number field<br>• Entry Number links to transactions<br>• Offer Number links to campaign master data<br>• Description = Offer Name for reporting |
| 1:27:44-End | Remaining | Wrap-up & Outstanding Items | • Need description field for offer names (confirmed available)<br>• Offer Type mapping documented (0-4 codes)<br>• Online requires manual SKU-to-campaign mapping for price-offs<br>• Shopify order-level discount only trackable element<br>• Multiple meetings scheduled for next week (ERP, WMS, OMS discussions) | • Anmar to receive final input files<br>• Test Shopify line-level discount data availability<br>• Create classification mapping table<br>• Document system limitations clearly |

---

## Key Concepts Discussed

### Revenue Attribution Framework
```
CAMPAIGN REVENUE CLASSIFICATION
├── Code-Based Discounts (~26% of stores revenue)
│   ├── Loyalty Programs
│   │   ├── ESAAD20 (20% off)
│   │   ├── FAZAA15 (15% off)
│   │   └── EKTPS2024 (Employee discount)
│   ├── Store Codes (SR20, STORE20)
│   └── Clearance Codes (CLEAR40, CLEAR80)
├── Thematic Campaigns (~24% of stores revenue)
│   ├── Direct Price-Offs (product-level discounts)
│   ├── Spend & Save (threshold: spend 300 get 14% off)
│   └── Category Mechanics (Buy 2 Get 20% off)
├── Brand Campaigns
│   ├── Flat % off entire brand range
│   └── Examples: Hills 15% off, Lily's Kitchen 15% off
├── Clearance (~1% of revenue)
│   └── Near-expiry or end-of-life products
└── Organic (~47% of stores revenue)
    └── Full-price sales with NO discount
```

### Baseline Calculation Methodology
```
BASELINE CALCULATION (Current State - Problematic)
├── Manual Selection Approach
│   ├── Choose recent month (not year-ago)
│   ├── Match day-of-week pattern
│   ├── Example: July 1-7 (Tue-Mon) → Find similar Tue-Mon in Jan
│   └── Rationale: Baselines change due to business growth
├── Challenges
│   ├── Too many active campaigns (hard to find "clean" baseline)
│   ├── Seasonal variations not captured
│   ├── Subjective selection (acknowledged as "gray area")
│   └── Weekend vs weekday sales patterns differ significantly
└── Proposed Solution: Slim Stock Integration
    ├── Use demand forecasting data (3-month forward)
    ├── Convert quantities to revenue (Qty × Standard Price)
    ├── Map to margin (Qty × Unit Cost)
    └── Get "zero-discount" baseline automatically
```

### Data Architecture
```
DATA SOURCES & FLOW
├── OFFLINE (Stores)
│   ├── ERP - Discount Ledger Entry
│   │   ├── Entry Number (PK)
│   │   ├── Document Number (Order ID)
│   │   ├── Offer Number (Campaign Code)
│   │   ├── Offer Type (0-4: Promotion/Deal/Multibuy/Mix/Total)
│   │   ├── Discount Amount
│   │   └── Sales Amount
│   ├── ERP - Item Ledger
│   │   ├── Entry Number
│   │   ├── Item Number (SKU)
│   │   ├── Quantity
│   │   └── Unit Cost
│   ├── ERP - Periodic Discount (Master)
│   │   ├── Offer Number (Join Key)
│   │   ├── Description (Offer Name)
│   │   └── No Document Number (master table)
│   └── Power BI - Sales Reports
│       └── Aggregated sales data
├── ONLINE (Shopify)
│   ├── Shopify Orders
│   │   ├── Order ID
│   │   ├── Discount Code (ORDER-LEVEL ONLY)
│   │   ├── Total Discount Amount
│   │   └── Line Items (with prices)
│   ├── LIMITATION: No line-item discount attribution
│   ├── LIMITATION: Price-offs show as regular price (no discount tracking)
│   └── Manual Mapping Required
│       └── SKU sold during campaign dates → Campaign revenue
└── PLANNED: Slim Stock Integration
    ├── Demand Forecasts (Quantity-based)
    ├── 3-month forward visibility
    ├── Needs: RSP mapping + Margin mapping
    └── Output: Baseline revenue without promotions
```

### Campaign Performance Metrics
```
KEY METRICS TRACKED
├── Revenue Metrics
│   ├── Actual Revenue (ACH) - Post-discount transaction value
│   ├── Target Revenue (TGT) - Planned campaign revenue
│   ├── Achievement % (ACH%) - Actual/Target
│   └── Potential Revenue - Pre-discount value (Qty × Standard Price)
├── Profitability Metrics
│   ├── Margin (AED) - Gross profit after discounts
│   ├── Margin % - Gross profit as % of revenue
│   └── Discount % - Discount as % of potential revenue
├── Performance Metrics
│   ├── Uplift - Additional units sold vs baseline
│   ├── Uplift X - Multiplier (e.g., 1.3 = 30% more than baseline)
│   ├── Orders - Number of transactions
│   └── Items - Number of units sold
└── Online-Specific Metrics
    ├── Traffic (Store Visitors)
    ├── Conversion Rate % (CR%)
    └── Average Order Value
```

### Technical Mapping Requirements
```
DATA JOINS & TRANSFORMATIONS
├── Offline Campaign Attribution
│   ├── Discount Entry [Entry Number + Offer Number]
│   ├── → JOIN → Periodic Discount [Offer Number = Description]
│   ├── → JOIN → Item Ledger [Entry Number = Item + Qty + Cost]
│   └── → Classification Table [Description → Campaign Type]
├── Online Campaign Attribution
│   ├── Shopify Orders [Order ID + Discount Code]
│   ├── → Limited to promo code campaigns only
│   └── → Manual mapping for thematic/price-off campaigns
└── Offer Type Decoding
    ├── 0 → "Promotion"
    ├── 1 → "Deal"
    ├── 2 → "Multibuy"
    ├── 3 → "Mix & Match"
    └── 4 → "Total Discount"
```

---

## Most Important Insights

### Strategic Insights

> "We spend about roughly 14%, 13% on discounts out of the overall revenue... for affiliates and B2B we might end up spending around 0.5-1% of the revenue on discount" - Sachin (4:30)

**Context:** ~50% of revenue comes from promotional sales, with 13-14% of total revenue spent on discounts across channels. This highlights the massive business impact of campaign effectiveness.

---

> "Drafting that report takes a lot of time. Me to generate the first report which I showed you, the PowerPoint presentation, it takes at least about good five hours" - Sachin (41:47)

**Context:** Weekly manual reporting burden of 5 hours represents significant opportunity for automation ROI.

---

> "For me, the only problem which we have is we don't have a map to map this back to revenue and margins... quantities is important, but the only problem which we have is we don't have a map" - Sachin (24:51)

**Context:** Slim Stock provides demand forecasts but only in quantities, not revenue values. This is the key blocker for automated baseline calculation.

---

### Technical Discoveries

> "In Shopify, each and every discount which we apply, we update it as a campaign, right... Now if we want to run a price-off, we update the price and update it as a discount. So you can't track after this" - Sachin (1:04:40)

**Context:** Critical limitation discovered - Shopify doesn't track direct price reductions as discounts, only promo codes. This means thematic campaigns require manual mapping.

---

> "Customers can't combine two codes together. That is 100%... What's happening I'll explain: In Shopify, it never captures if we put [a price-off]... It will just reflect the amount" - Sachin (1:08:28)

**Context:** System architecture prevents multiple promo codes but allows price-offs + promo code. However, only the promo code is tracked, creating blind spots.

---

### Process Insights

> "I always try to simulate not the dates... I will not try to simulate the revenue from first to seventh January, but rather I will see what are the dates of first to seventh July, does it start on a Tuesday and does it end on a Saturday" - Sachin (21:25)

**Context:** Day-of-week matching is critical because weekend sales (150K AED) are nearly double weekday sales (80-90K AED) in stores.

---

> "January was a month where we intentionally spent less in discounts. So that is a good enough indication for me to check how much we would be generating if we don't run discount again" - Sachin (22:39)

**Context:** January serves as a "clean baseline" month with minimal promotions, making it the preferred reference point despite being 6 months old.

---

### Data Quality Insights

> "For stores, posting and document date doesn't differ. It's the same revenue because we post the order and we get paid on the same day. But online, posting date is based on when we actually deliver the order" - Sachin (36:40)

**Context:** Document date must be used consistently for campaign attribution because it represents when the customer made the purchase decision (during campaign period).

---

> "In the SQL, you cannot know. SQL always stores the code. It has unique code only for drop downs... Application doesn't understand that, SQL always stores the code" - IT Team (58:04)

**Context:** All ERP dropdown fields store numeric codes in database but show text labels in UI. Requires mapping tables for all reference data.

---

## Action Items Tracker

| Owner | Action | Priority | Context | Status |
|-------|--------|----------|---------|--------|
| Sachin | Provide December 2024 input files (ERP Discount Ledger, Item Ledger, Shopify export, BI reports) | HIGH | Case study dataset for development | Pending |
| Anmar | Map ERP data sources to Power BI data model | HIGH | Identify which tables already exist vs need to be added | Pending |
| IT Team | Document Offer Type code definitions (0-4 mapping) | HIGH | Codes: 0=Promotion, 1=Deal, 2=Multibuy, 3=Mix&Match, 4=Total Discount | Complete |
| Anmar | Bring "Periodic Discount" table into Power BI data warehouse | HIGH | Contains Offer Number → Description mapping | Pending |
| Sarah | Set up Shopify data sync to Power BI via API | HIGH | Need: orders.discount_code.code at minimum | Pending |
| Anmar | Test if Shopify line-item discount data exists anywhere | MEDIUM | Critical for understanding online discount attribution limitations | Pending |
| Sachin | Create campaign classification mapping table | MEDIUM | Excel file: Offer Name/Description → Campaign Type (Code/Thematic/Brand/etc.) | Pending |
| Anmar | Investigate Slim Stock API integration with Power BI | MEDIUM | Need to convert quantity forecasts to revenue (Qty × RSP) | Pending |
| Sachin | Provide example order with partial line-item discounts (online) | LOW | Test case to validate Shopify line-level discount capability | Pending |
| Sachin | Schedule practical walkthrough session of manual Excel process | MEDIUM | Show Anmar exactly how data is manipulated currently | Scheduled |
| Anmar | Document system limitations clearly for stakeholders | MEDIUM | Especially online thematic campaign tracking gaps | Pending |
| Team | Plan follow-up ERP/WMS/OMS integration sessions | LOW | Broader system understanding needed | Scheduled |

---

## Key Challenges Identified

| Challenge | Impact | Proposed Solution |
|-----------|--------|-------------------|
| **Baseline calculation is subjective** | Campaign uplift metrics unreliable; can't prove true incrementality | Integrate Slim Stock demand forecasts into BI, convert quantities to revenue using standard pricing |
| **Shopify only tracks discount codes at order level** | Cannot attribute discounts to specific line items in multi-product orders | Accept limitation; focus automation on code-based campaigns; manual mapping for thematic |
| **Shopify doesn't track direct price reductions** | Thematic campaigns (price-offs) invisible in system; no automated tracking possible | Manual SKU-to-campaign mapping table based on campaign dates; accept as ongoing process |
| **Offer Type codes undocumented** | Cannot use Offer Type for classification without knowing what codes mean | IT provided mapping (now complete); create CASE statement in BI |
| **Campaign classification not in ERP** | Must manually categorize into Code/Thematic/Brand/Mechanics | Create lookup table mapping Offer Description to Campaign Type; update as new campaigns launch |
| **5 hours/week manual reporting** | Significant time drain; delays in availability; error-prone | Build automated Power BI dashboards for both campaign summary and performance reports |
| **Data scattered across 3+ sources** | Complex data wrangling; no single source of truth | Consolidate all sources into Power BI with proper joins and transformations |
| **Weekly sales patterns highly variable** | Weekend vs weekday sales differ by 2x; monthly patterns (payday effect) | Match day-of-week when selecting baseline periods; never use calendar date matching |
| **Multiple concurrent campaigns** | Hard to isolate individual campaign effects; attribution unclear | System auto-applies best discount; track at campaign level, not transaction level |
| **Business growth makes historical baselines outdated** | Year-over-year comparisons invalid (May: 3.4M→3.9M = 15% growth) | Always use recent month (within 3 months); never use year-ago data for baseline |

---

## Technical/Domain Discoveries

### ERP Data Model

**Discount Ledger Entry Table:**
- **Entry Number** - Primary key, links to Item Ledger
- **Document Number** - Order identifier
- **Offer Number** - Campaign code (e.g., "82", "E-82")
- **Offer Type** - Numeric code (0-4):
  - 0 = Promotion
  - 1 = Deal
  - 2 = Multibuy
  - 3 = Mix & Match  
  - 4 = Total Discount
- **Discount Amount** - Money given as discount
- **Sales Amount** - Actual revenue (post-discount)

**Periodic Discount Table (Master):**
- **Offer Number** - Join key to Discount Ledger
- **Description** - Offer Name (e.g., "Affiliate 50", "ESAAD20")
- **No transaction-level data** - Pure reference table

**Item Ledger:**
- **Entry Number** - Links to Discount Ledger
- **Item Number** - SKU identifier
- **Quantity** - Units sold
- **Unit Cost** - For margin calculation

### Shopify Data Structure

**Orders Table:**
- **Order ID** - Primary key
- **Discount Code** - `orders.discount_code.code` (ORDER-LEVEL only)
- **Total Discount Amount** - Sum of all discounts on order
- **Line Items** - Individual products in order
  - Contains prices but NOT individual discounts
  - No attribution of which items got discount

**Critical Limitations:**
- Cannot track partial discounts within an order
- Direct price reductions (price-offs) show as regular prices, not discounts
- Only promo code discounts are trackable
- Line-item level discount data does not exist in accessible format

### Campaign Execution Workflow

**Code-Based Campaigns (Trackable):**
1. Create discount offer in ERP with unique code
2. Communicate code to stores/customers
3. Code entered at POS or checkout
4. Discount automatically applied
5. Tracked in Discount Ledger with Offer Number

**Thematic Campaigns - Price-Offs (Not Fully Trackable Online):**
1. Select products for promotion
2. **Offline**: Create discount in ERP → Tracked normally
3. **Online**: Update product price in Shopify → NOT tracked as discount
4. **Manual workaround**: Map SKUs sold during campaign period to campaign revenue
5. Cannot isolate campaign effect from organic sales

**Spend & Save / Mechanics:**
- Condition-based triggers (e.g., cart total > 300 AED → 14% off)
- System evaluates at checkout
- Best available discount auto-applied if multiple campaigns active
- Customer sees one discount, receives highest benefit

### Slim Stock Forecasting System

**Current State:**
- Provides 3-month forward demand forecasts
- Granularity: By product, category, potentially by channel
- Output: **Quantities only** (units expected to sell)
- Updated regularly based on sales patterns

**Gap for Campaign Planning:**
- No revenue values (Quantity × Price)
- No margin values (Quantity × Unit Cost)
- Cannot be used directly for baseline calculation

**Proposed Integration:**
- Extract forecast quantities from Slim Stock API
- Join to product master for RSP (Retail Selling Price)
- Join to product master for Unit Cost
- Calculate: Baseline Revenue = Forecast Qty × RSP
- Calculate: Baseline Margin = (RSP - Unit Cost) × Forecast Qty
- Use as "zero-discount" baseline for campaign planning

### Business Patterns Discovered

**Weekly Sales Patterns:**
- **Weekdays** (Mon-Fri): 80,000-90,000 AED/day in stores
- **Weekends** (Sat-Sun): 140,000-150,000 AED/day in stores
- **Ratio**: Weekend sales ~1.7x weekday sales

**Monthly Sales Patterns:**
- **Last week of month + First week of next month** = Peak sales period
- **Reason**: Payday effect (more disposable income)
- **Weeks 2-3**: Lower sales, slower weeks
- **Implication**: Most aggressive discounts scheduled for last week + first week

**Promotional Revenue Mix:**
- Stores: ~53% promoted, 47% organic
- Online: ~57% promoted, 43% organic
- Overall discount spend: 12-14% of total revenue

### Document Date vs. Posting Date

**Document Date:**
- When customer placed the order
- Used for campaign attribution
- Same for stores and online

**Posting Date:**
- When revenue was recognized (delivered)
- Differs between channels:
  - **Stores**: Same as document date (instant)
  - **Online**: Delivery date (1-3 days later typically)

**Why Document Date Matters:**
- Customer decision influenced by campaign active on document date
- If customer orders during campaign but delivery is after, still counts toward campaign
- Must use document date consistently to measure campaign effectiveness

---

## Conceptual Breakthroughs

### 1. **Baseline Calculation is More Art Than Science Currently**

**The Challenge:**
The business runs so many campaigns continuously that finding a "clean" baseline is nearly impossible. Sachin's current approach of manually selecting similar weeks from recent months is subjective and varies based on judgment calls.

**The Insight:**
This isn't a data problem - it's a methodology problem. The solution isn't better Excel formulas; it's integrating demand forecasting (Slim Stock) to establish a theoretical "zero-discount" baseline. This would transform uplift calculation from subjective estimation to objective measurement.

**Why It Matters:**
Without reliable baselines, the business cannot truly measure campaign ROI or prove incrementality. Are campaigns generating new sales or just discounting sales that would happen anyway?

---

### 2. **Online Discount Tracking Has Fundamental Architecture Limitations**

**The Challenge:**
Shopify's architecture only exposes discount codes at the order level, and direct price reductions aren't tracked as discounts at all.

**The Insight:**
This isn't a data extraction problem that can be solved with better APIs - it's a fundamental system limitation. Thematic campaigns (price-offs) are invisible to the reporting layer because Shopify treats them as the "normal" price, not a discount.

**Why It Matters:**
The team must accept that ~22% of online revenue (thematic campaigns) will always require manual classification. Automation efforts should focus on the 35% from code-based campaigns where data IS available. Setting realistic expectations prevents wasted effort on impossible automation.

---

### 3. **Day-of-Week Matching is More Important Than Calendar Date Matching**

**The Challenge:**
When selecting baseline periods, the instinct is to compare "same date last year" (e.g., July 1, 2024 vs July 1, 2025).

**The Insight:**
Weekend vs. weekday sales differ by 70% in stores. A campaign running Tuesday-Monday must be compared to a baseline that's also Tuesday-Monday, regardless of calendar dates. The day-of-week pattern dominates the calendar date pattern.

**Why It Matters:**
Using calendar date matching would systematically over- or under-state campaign performance depending on where weekends fall. This explains why Sachin goes to the effort of matching day-of-week patterns in his manual process.

---

### 4. **The Business Has Grown Too Fast for Year-Over-Year Baselines**

**The Challenge:**
Standard retail practice is to compare performance to "same period last year."

**The Insight:**
When a business is growing 15% YoY (May: 3.4M → 3.9M), last year's performance is systematically too low as a baseline. This makes every campaign look successful even if it's actually cannibalizing organic sales.

**Why It Matters:**
Recent months (1-3 months old) are better baselines than year-ago data, even though they may have had some promotions. The business needs to track growth rate separately from campaign effectiveness.

---

### 5. **Manual Classification is Unavoidable but Can Be Systematized**

**The Challenge:**
Campaign types (Code/Thematic/Brand/Mechanics/Clearance) don't exist as fields in any system.

**The Insight:**
This is business logic, not system logic. No ERP or e-commerce platform will have this classification because it's specific to The Petshop's commercial strategy. However, it can be captured in a simple lookup table that maps offer names/descriptions to campaign types.

**Why It Matters:**
Instead of classifying every transaction manually, Sachin can maintain one small reference table (maybe 50-100 rows) that automatically classifies thousands of transactions. The manual work shifts from transaction-level to campaign-level.

---

## Meeting Effectiveness Score

### Session 1 (June 26, 2025): **8.5/10**

**Strengths:**
✅ **Clear problem definition** - 5 hours/week manual process clearly articulated  
✅ **Stakeholder engagement** - Sachin openly shared current process and pain points  
✅ **Data source identification** - All three input sources (ERP × 2, Shopify) documented  
✅ **Baseline challenge acknowledged** - Both parties recognized this as complex, not simple  
✅ **Future-state vision discussed** - Slim Stock integration identified as strategic solution  
✅ **Concrete next steps** - December 2024 case study approach agreed  

**Areas for Improvement:**
⚠️ **Shopify limitations not fully explored** - "Tricky part" deferred to later discussion  
⚠️ **Timeline not established** - No dates or milestones set for deliverables  
⚠️ **Resource requirements unclear** - Dependencies on IT/Sarah not mapped out  
⚠️ **Classification logic not captured** - Manual process mentioned but not documented  

---

### Session 2 (October 24, 2025): **9/10**

**Strengths:**
✅ **Deep technical discovery** - Discount Ledger, Periodic Discount, and joins fully mapped  
✅ **Limitations documented** - Shopify line-item gap thoroughly investigated  
✅ **System constraints accepted** - Team realistic about what can/cannot be automated  
✅ **Reference data decoded** - Offer Type codes (0-4) obtained from IT  
✅ **Multiple test cases examined** - Real examples used to validate data structures  
✅ **Visual documentation** - Screenshots taken for future reference  
✅ **Collaborative problem-solving** - IT team pulled in to answer dropdown mapping question  

**Areas for Improvement:**
⚠️ **Classification mapping table not created** - Still pending as action item  
⚠️ **Slim Stock discussion postponed** - Mentioned but not advanced from Session 1  

---

### Combined Project Effectiveness: **8.8/10**

**Overall Strengths:**
✅ Methodical discovery process (Session 1 = big picture, Session 2 = deep dive)  
✅ Realistic about limitations (accepted Shopify gaps, didn't chase impossible solutions)  
✅ Data-driven approach (used December 2024 as case study)  
✅ Strong collaboration (Sachin, Anmar, IT team all contributing)  
✅ Technical rigor (exact table names, field names, join keys all documented)  

**Overall Improvement Opportunities:**
⚠️ Project timeline still undefined (when will dashboard be ready?)  
⚠️ Resource allocation unclear (who else needs to be involved? how much IT time?)  
⚠️ Success metrics not established (beyond "save 5 hours/week")  

---

## Key Quotes by Topic

### Current State Pain Points

> "Drafting that report takes a lot of time. Me to generate the first report which I showed you, the PowerPoint presentation, it takes at least about good five hours" - Sachin (41:47, Session 1)

> "For me, the only problem which we have is we don't have a map to map this back to revenue and margins, because quantities is important, but the only problem is we don't have a map" - Sachin (24:51, Session 1)

> "This uplift is questionable, because right now we don't have proper way to practice. This is something I need to... how to track baseline" - Sachin (10:07, Session 1)

---

### Technical Limitations

> "In Shopify, each and every discount which we apply, we update it as a campaign. Now if we want to run a price-off, we update the price and update it as a discount. So you can't track after this" - Sachin (1:04:40, Session 2)

> "In the SQL, you cannot know what is the name. SQL doesn't show the values. What you're seeing in application will store as 0,1,2,3,4" - IT Team (58:04, Session 2)

> "BI doesn't have the promo code information... We will have to export from Shopify to get discount codes" - Sachin (38:28, Session 1)

---

### Business Context

> "Generally first week and the second week of last week and the first week of the month are peak weeks for us, because people have more disposable income because of payday week" - Sachin (16:50, Session 1)

> "On a weekday, they might do somewhere close to 80,000 to 90,000 dirhams in sales. But during weekends, Saturday and Sunday, this number goes up to 150 and 140,000" - Sachin (22:13, Session 1)

> "We spend about roughly 14%, 13% on discounts out of the overall revenue" - Sachin (4:30, Session 2)

---

### Solution Approach

> "If you can create a dashboard which on an item level we should be able to have filters. Then what we should be able to do is put a specific date range. If we put a specific date range, we should be able to get automatically how much did we sell without any promotion during this period - revenue, margin, and the number of items and orders" - Sachin (27:27, Session 1)

> "What we can do is if we can get that information flowing to BI, not on quantity level but on a value level - revenue and margin level - then we exactly know how much because this is demand forecast" - Sachin (29:21, Session 1)

> "We can map. Just you can put an Excel sheet, small Excel sheet: if this then that, if this then that. And we can apply kind of classification map" - Anmar (31:46, Session 2)

---

### Baseline Methodology

> "I always try to simulate not the dates... I will see what are the dates of first to seventh July, does it start on a Tuesday and does it end on a Saturday. If it starts on the first Tuesday and ends on the first Saturday, then I will try to take the baseline of January of first Tuesday to first Saturday" - Sachin (21:25, Session 1)

> "January was a month where we intentionally spent less in discounts. So that is a good enough indication for me to check how much we would be generating if we don't run discount again" - Sachin (22:39, Session 1)

> "If we look at last May we did 3.4 million in revenue, but this May we did 3.9 million. So what happens is now if I take this baseline, this baseline might be too outdated for us. The amount of customers shopping during July versus the amount of customers shopping with us right now has changed, has increased" - Sachin (20:15, Session 1)

---

### Collaboration & Process

> "For now, at the next step, I think we will just need from Sarah to give us the Shopify data exactly. We need Shopify orders" - Anmar (1:19:58, Session 2)

> "I think let's just focus on one promotion. I think that would be easier. Let's just take one campaign and understand the entire flow" - Sachin (15:55, Session 1)

> "You should not build a system based on one exception" - Team member (1:12:24, Session 2)

---

## Next Steps & Recommendations

### Immediate Priorities (Next 2 Weeks)

1. **Complete Data Source Mapping**
   - Anmar: Verify all required tables exist in Power BI data warehouse
   - Add missing tables: Periodic Discount, Periodic Discount Line
   - Document current data model vs. required data model

2. **Create Classification Mapping Table**
   - Sachin: Export all unique Offer Names/Descriptions from last 12 months
   - Classify each into: Code / Thematic / Brand / Mechanics / Clearance
   - Provide to Anmar as Excel file for lookup table in BI

3. **Shopify Data Sync**
   - Sarah: Set up automated Shopify → Power BI data pipeline
   - Minimum required: orders.discount_code.code field
   - Test with December 2024 data first

4. **December 2024 Case Study**
   - Sachin: Provide all input files (ERP Discount Ledger, Item Ledger, Shopify export, BI reports)
   - Anmar: Build prototype dashboard using December data
   - Validate: Does it match Sachin's manual report?

### Short-Term (1-2 Months)

5. **Build Campaign Summary Dashboard - Offline**
   - Start with stores only (data is cleaner)
   - Implement joins: Discount Entry → Periodic Discount → Item Ledger
   - Add classification logic via lookup table
   - Show: Revenue, Margin, Discount by campaign type

6. **Build Campaign Summary Dashboard - Online**
   - Focus on code-based campaigns only (trackable)
   - Accept limitations on thematic campaigns
   - Document manual process for thematic campaign mapping

7. **Campaign Performance Dashboard (Stage 2)**
   - Add baseline comparison logic
   - Calculate uplift (with caveats documented)
   - Track ACH% (Achievement vs Target)
   - Include traffic & conversion for online

### Medium-Term (3-6 Months)

8. **Slim Stock Integration Research**
   - Investigate API availability and structure
   - Map forecast quantities to product master (RSP, Cost)
   - Prototype: Baseline Revenue = Forecast Qty × RSP
   - Test accuracy vs. Sachin's manual baseline selection

9. **Baseline Methodology Enhancement**
   - Compare approaches: Manual selection vs. Slim Stock forecast vs. Trailing average
   - Run parallel test for 1-2 months
   - Document pros/cons of each approach
   - Select methodology and document assumptions

10. **Automation & Scheduling**
    - Set up automated data refresh schedule
    - Create email distribution for weekly reports
    - Build mobile-friendly views for executives
    - Measure time saved vs. 5-hour manual baseline

### Long-Term Considerations

11. **System Improvement Advocacy**
    - Document Shopify line-item discount gap to leadership
    - Build business case: "What decisions could we make with better data?"
    - Explore: Shopify app/plugin for line-item discount tracking
    - Alternative: Move thematic campaigns to code-based (generate dynamic codes)

12. **Advanced Analytics**
    - Customer-level campaign effectiveness (who responds to what?)
    - Cross-campaign cannibalization analysis
    - Optimal discount level by category (price elasticity)
    - Predictive modeling: Forecast campaign ACH% before launch

---

## Risk & Dependency Log

| Risk/Dependency | Mitigation | Owner |
|-----------------|------------|-------|
| Slim Stock API may not be accessible | Start with manual baseline process; improve incrementally | Anmar |
| Shopify data sync delayed by IT priorities | Use manual exports short-term; prioritize automation | Sarah |
| Classification mapping incomplete | Start with top 80% of revenue; add others later | Sachin |
| ERP Periodic Discount table not in warehouse | Request IT to add table to data pipeline | Anmar + IT |
| December 2024 files not representative | Use multiple months (Nov, Dec, Jan) for validation | Sachin |
| Baseline methodology disagreement | Run parallel approaches, compare results, decide together | Sachin + Anmar |
| Online thematic campaigns always manual | Accept as limitation; document clearly for stakeholders | Sachin |
| Scope creep (requests for more reports) | Define Phase 1 scope clearly; park additional requests | Anmar |

---

## Appendix: Technical Reference

### ERP Dropdown Code Mappings

**Offer Type Codes:**
- 0 = Promotion
- 1 = Deal
- 2 = Multibuy
- 3 = Mix & Match
- 4 = Total Discount

**How to Handle in BI:**
```sql
CASE 
    WHEN Offer_Type = 0 THEN 'Promotion'
    WHEN Offer_Type = 1 THEN 'Deal'
    WHEN Offer_Type = 2 THEN 'Multibuy'
    WHEN Offer_Type = 3 THEN 'Mix & Match'
    WHEN Offer_Type = 4 THEN 'Total Discount'
    ELSE 'Unknown'
END AS Offer_Type_Name
```

### Data Join Logic

**Offline Campaign Attribution:**
```
Discount_Ledger_Entry (Entry_Number, Offer_Number, Discount_Amount, Sales_Amount)
    JOIN Periodic_Discount (Offer_Number, Description)
        ON Discount_Ledger_Entry.Offer_Number = Periodic_Discount.Offer_Number
    JOIN Item_Ledger (Entry_Number, Item_Number, Quantity, Unit_Cost)
        ON Discount_Ledger_Entry.Entry_Number = Item_Ledger.Entry_Number
    LEFT JOIN Classification_Lookup (Offer_Description, Campaign_Type)
        ON Periodic_Discount.Description = Classification_Lookup.Offer_Description
```

**Calculated Fields:**
- **Margin** = Sales_Amount - (Quantity × Unit_Cost)
- **Margin %** = Margin / Sales_Amount
- **Potential Revenue** = Quantity × Standard_Price
- **Discount %** = Discount_Amount / Potential_Revenue

### Sample Classification Lookup Table

| Offer Description | Campaign Type |
|-------------------|---------------|
| ESAAD20 | Code |
| FAZAA15 | Code |
| EKTPS2024 | Code |
| Spend 250 & Get 14% Off | Thematic - Spend & Save |
| Wet Cat Food: Buy Any 6 & Get 15% Off | Thematic - Mechanics |
| LILY's KITCHEN - Flat 15% Off | Brand |
| HILL's - Flat 15% Off | Brand |
| Clearance | Clearance |
| Affiliate 50 | Code |

---

**Analysis Complete**

📥 **Download Full Report:**
[Meeting_Segmentation_Campaign_Performance_20250626_20251024.md](computer:///mnt/user-data/outputs/Meeting_Segmentation_Campaign_Performance_20250626_20251024.md)

**File Size:** 83KB  
**Sections Included:** 9 comprehensive sections  
**Action Items:** 12 items identified with clear ownership  
**Meeting Duration:** ~133 minutes analyzed across 2 sessions  
**Key Insights:** 9 strategic and technical breakthroughs documented  

**This comprehensive analysis is ready to share with stakeholders, archive for future reference, or use as a project blueprint for the campaign performance dashboard automation.**

---