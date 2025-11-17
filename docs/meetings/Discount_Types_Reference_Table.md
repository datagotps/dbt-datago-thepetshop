# The Petshop - Discount Types Reference Guide

**Based on Campaign Performance Meetings (June 26 & October 24, 2025)**

---

## Complete Discount Types Overview

| Discount Type | Classification | Description | Mechanism | Tracking Method | Revenue Share | Example Codes/Names | Typical Discount % |
|--------------|----------------|-------------|-----------|-----------------|---------------|---------------------|-------------------|
| **ESAAD Loyalty** | Code-Based | Government employee discount program | Customer shows ESAAD card at checkout, store enters code | ERP Discount Ledger via code "ESAAD20" | 11% of stores revenue | ESAAD20 | 20% |
| **FAZAA Loyalty** | Code-Based | Military/police employee discount program | Customer shows FAZAA card at checkout, store enters code | ERP Discount Ledger via code "FAZAA15" | 10% of stores revenue | FAZAA15 | 15% |
| **Employee Discount** | Code-Based | Staff purchase discount | Employee enters code at checkout | ERP Discount Ledger via code "EKTPS2024" | 4% of stores revenue | EKTPS2024 | 15% |
| **Store General Codes** | Code-Based | General promotional codes for in-store use | Store staff enters code at POS | ERP Discount Ledger | <1% of revenue | STORE20, SR20 | 20% |
| **Clearance Codes** | Code-Based | Near-expiry or end-of-life product codes | Manual code entry at checkout | ERP Discount Ledger | <1% of revenue | CLEAR40, CLEAR80, CLEAR20, DAM40 | 20-80% |
| **Online Promo Codes** | Code-Based | Customer-entered codes for online orders | Customer types code at checkout | Shopify Orders (order level) | 35% of online revenue | AN14, PET15, HBD20, SHOP20, TREAT20 | 14-20% |
| **Affiliate Codes** | Code-Based | Partner/influencer promotional codes | Customer enters affiliate code | Shopify Orders + ERP | <1% of revenue | Affiliate 50, CMENA | 15-50% |
| **Spend & Save** | Thematic - Threshold | Automatic discount when cart reaches threshold | System applies automatically when conditions met | ERP (offline) / Manual mapping (online) | 15% of stores revenue | "Spend 250 & Get 14% Off" | 14% |
| **Everything Under AED X** | Thematic - Price Point | All products below certain price get discount | Direct price reduction | Manual classification by date range | 2% of revenue | "Everything Under AED 14" | 15% |
| **Category Mechanics** | Thematic - Mechanics | Buy X quantity from category, get discount | System applies when conditions met | ERP (offline) / Manual mapping (online) | 2-5% of revenue | "Wet Cat Food: Buy 6 Get 15% Off"<br>"Treats: Buy 5 Get 20% Off" | 15-20% |
| **Product-Specific Mechanics** | Thematic - Mechanics | Buy X of specific product, get discount | System applies when conditions met | ERP (offline) / Manual mapping (online) | 1% of revenue | "Dog Beds & Cat Trees: Flat 20% Off" | 20% |
| **Brand Campaigns** | Thematic - Brand | Flat discount on entire brand range | Direct price reduction OR code | ERP (if code) / Manual mapping (if price) | 1-3% per brand | "LILY's KITCHEN - Flat 15% Off"<br>"HILL's - Flat 15% Off"<br>"ADDICTION - 15% Off" | 15% |
| **Everyday Low Prices (EDLP)** | Thematic - Price Strategy | Permanent lower prices on select SKUs | Direct price reduction (not tracked as discount) | Manual tracking via campaign dates | 12% of online revenue | "Everyday Low Prices" campaign | 10-25% |
| **Clearance - Near Expiry** | Clearance | Products approaching expiration date | Direct price markdown | Manual classification | <1% of revenue | "Near Expiry" | 35-80% |
| **Organic (No Discount)** | Organic | Full-price sales with no promotion | Standard pricing | Residual calculation | 47% stores, 43% online | N/A | 0% |

---

## Detailed Breakdown by Classification

### 1. CODE-BASED DISCOUNTS (~26% of Stores Revenue, ~35% of Online Revenue)

**How They Work:**
- Customer (or store staff) enters a specific code at checkout
- System validates the code and applies corresponding discount
- Each code is pre-configured in ERP or Shopify
- **Trackable**: YES - Every transaction has offer number/discount code

**Key Characteristics:**
- ✅ Fully trackable in both offline and online
- ✅ Can measure revenue, margin, discount precisely
- ✅ Clear campaign attribution
- ⚠️ Online: Only at order level (not per line item)

**Examples from Meetings:**

| Code | Full Name | Channel | Target Audience | Discount | Usage Pattern |
|------|-----------|---------|-----------------|----------|---------------|
| ESAAD20 | ESAAD Loyalty Program | Stores | Government employees | 20% | Customer shows card, staff enters code |
| FAZAA15 | FAZAA Loyalty Program | Stores | Military/Police | 15% | Customer shows card, staff enters code |
| EKTPS2024 | Employee Discount | Both | Company employees | 15% | Employee identifies themselves |
| AN14 | Anniversary Countdown | Online | All customers | 14% | Promotional email/SMS campaign |
| PET15 | General Promo | Online | New/All customers | 15% | Welcome offer |
| HBD20 | Birthday Discount | Online | Birthday customers | 20% | Birthday club members |
| SHOP20 | Shopping Festival | Online | All customers | 20% | Seasonal campaign |
| CLEAR40 | Clearance 40% | Stores | All customers | 40% | Near-expiry products |
| CLEAR80 | Clearance 80% | Stores | All customers | 80% | Damaged/discontinued items |

---

### 2. THEMATIC CAMPAIGNS (~24% of Stores Revenue, ~22% of Online Revenue)

**How They Work:**
- System automatically applies discount when conditions are met
- OR price is directly reduced at product level
- Customer doesn't need to enter any code
- **Trackable Offline**: YES via ERP
- **Trackable Online**: PARTIAL - Only if code-based; Manual mapping if price-based

**Sub-Types:**

#### A. SPEND & SAVE (Threshold-based)
**Mechanism:** Spend AED X, get Y% off entire cart

| Campaign Name | Threshold | Discount | Channel | How It's Applied |
|---------------|-----------|----------|---------|------------------|
| Spend 250 & Get 14% Off | AED 250 | 14% | Stores | Automatic at checkout when cart total ≥ 250 |
| Spend 300 & Get 14% Off | AED 300 | 14% | Online | Automatic at checkout when cart total ≥ 300 |

**Revenue Contribution:** 15% of stores revenue in example month (May)

---

#### B. CATEGORY MECHANICS (Buy X, Get Discount)
**Mechanism:** Purchase specific quantity from category, receive discount

| Campaign Name | Condition | Discount | Category | Channel |
|---------------|-----------|----------|----------|---------|
| Wet Cat Food: Buy Any 6 & Get 15% Off | Buy 6+ items | 15% | Wet Cat Food | Both |
| Treats: Buy Any 5 & Get 20% Off | Buy 5+ items | 20% | Pet Treats | Stores |
| Dog Beds & Cat Trees: Flat 20% Off | Buy any | 20% | Dog Beds & Cat Trees | Both |

**Revenue Contribution:** 2-5% of revenue per campaign

---

#### C. BRAND CAMPAIGNS
**Mechanism:** Flat discount on all products from specific brand

| Brand | Discount | Duration (Example) | Implementation Method | Revenue Impact |
|-------|----------|-------------------|----------------------|----------------|
| LILY's KITCHEN | 15% | Week-long | Code OR price reduction | 1% of stores revenue |
| HILL's | 15% | Week-long | Code OR price reduction | 1% of stores revenue |
| ADDICTION | 15% | Week-long | Code OR price reduction | <1% of revenue |
| TERRA CANIS | 20% | Week-long | Code OR price reduction | <1% of revenue |
| Royal Canin | Varies | Brand Week | Usually price reduction | Higher impact |

---

#### D. EVERYDAY LOW PRICES (EDLP)
**Mechanism:** Permanent reduced pricing on select high-demand SKUs

**Key Characteristic:** 
- NOT tracked as discount in Shopify (just shows as regular price)
- Requires manual mapping of which SKUs are on EDLP program
- Campaign tracked by date range

**Example Products (from Week 13 Report):**
- Royal Canin Natural Tuna Fillet Wet Cat Food - 156G
- Kit Cat Grain Free Tuna & Salmon Topper Wet Cat Food - 80G
- Royal Canin Sterilised Adult Dry Cat Food - 2KG
- Applaws Natural Tuna Fillet with Seaweed Wet Cat Food - 156G
- Royal Canin Adult Dry Dog Food variants

**Revenue Contribution:** 12% of online revenue (May example)

---

#### E. PRICE POINT CAMPAIGNS
**Mechanism:** All products under certain price get discount

| Campaign Name | Condition | Discount | Channel |
|---------------|-----------|----------|---------|
| Everything Under AED 14 | Product price ≤ 14 AED | 15% | Both |

**Revenue Contribution:** 2% of revenue

---

### 3. CLEARANCE (~1% of Revenue)

**Purpose:** Move near-expiry, damaged, or discontinued inventory

**Types:**

| Clearance Type | Reason | Typical Discount | Tracking Code | Channel |
|----------------|--------|------------------|---------------|---------|
| Near Expiry | Products approaching expiration | 35-80% | CLEAR40, CLEAR80 | Both |
| Damaged Packaging | Box/container damage | 40-60% | DAM40, CLEAR40 | Stores mainly |
| Discontinued | Product being phased out | 40-80% | CLEAR80 | Both |
| Seasonal Clearance | End of season stock | 20-40% | CLEAR20 | Both |

**Characteristics:**
- Very high discount percentages (can be 80%)
- Very low margin (can be negative: -183% margin in one example)
- Small revenue contribution but important for inventory management

---

### 4. ORGANIC (NO DISCOUNT) (~47% Stores, ~43% Online)

**Definition:** Sales at full retail price with no promotional discount

**Why It Matters:**
- This is the baseline for measuring campaign effectiveness
- Higher organic % = Less promotional dependency
- Target: Maximize organic revenue while using promotions strategically

**Calculation Method:**
Organic Revenue = Total Revenue - All Discounted Revenue

---

## Tracking Methodology Comparison

### OFFLINE (STORES) - ✅ Fully Trackable

| Data Source | What It Contains | How Used |
|-------------|------------------|----------|
| **ERP Discount Ledger** | Entry Number, Offer Number, Offer Type, Discount Amount, Sales Amount | Primary source for all discount tracking |
| **ERP Item Ledger** | Entry Number, Item Number, Quantity, Unit Cost | Links to Discount Ledger for margin calculation |
| **ERP Periodic Discount** | Offer Number, Description (Offer Name) | Maps offer codes to human-readable names |
| **Power BI Sales Reports** | Aggregated sales data | Overall business metrics |

**Join Logic:**
```
Discount_Ledger.Entry_Number = Item_Ledger.Entry_Number
Discount_Ledger.Offer_Number = Periodic_Discount.Offer_Number
```

**What Can Be Tracked:**
- ✅ Exact revenue per campaign
- ✅ Exact margin per campaign
- ✅ Exact discount given per campaign
- ✅ Line-item level detail
- ✅ Partial discounts within orders

---

### ONLINE - ⚠️ Partially Trackable

| Data Source | What It Contains | Limitations |
|-------------|------------------|-------------|
| **Shopify Orders** | Order ID, Discount Code, Total Discount Amount | ❌ Only ORDER level (not line-item)<br>❌ Doesn't track direct price reductions |
| **Manual Mapping** | Campaign dates + SKU lists | ⚠️ Manual process required for thematic campaigns |

**What Can Be Tracked:**

✅ **Code-Based Campaigns:**
- Revenue per promo code (order level)
- Discount amount per promo code
- Number of orders using each code

❌ **Thematic Campaigns (Price-Offs):**
- Cannot track automatically
- Requires manual classification: "SKUs X, Y, Z sold between Date A and Date B = Campaign Revenue"

⚠️ **Limitations:**
- If order has 3 items but only 1 is discounted → System shows discount at order level
- Cannot determine which specific item received the discount
- If product price changed from 500 to 450 → Shopify shows 450 (no discount tracked)

---

## Campaign Planning & Baseline

### How Sachin Plans Campaigns

**Step 1: Select Campaign Type**
- Choose: Code / Thematic / Brand / Mechanics

**Step 2: Define Campaign Parameters**
- Brand: Purina
- Discount: 15% off
- Duration: July 1-7 (Tuesday to Monday)
- Channel: Both stores and online

**Step 3: Establish Baseline**
- Find similar week from recent month (e.g., January)
- Match day-of-week pattern (Tuesday-Monday)
- Extract revenue/margin for those SKUs during baseline period
- This becomes the "zero discount" baseline

**Step 4: Set Target**
- Based on historical similar campaigns
- Example: Previous brand campaigns delivered 1.3x uplift
- Target Revenue = Baseline × 1.3

**Step 5: Track Performance**
- Actual Revenue vs Target = Achievement %
- Actual Revenue vs Baseline = Uplift

---

## Discount Overlap Rules

**What Happens When Multiple Campaigns Apply?**

> "The customer would get the highest whatever is beneficial for the customer will get triggered. It's by the system itself. So it's always only one discount" - Sachin

**Examples:**

| Scenario | Available Discounts | What Customer Receives |
|----------|-------------------|----------------------|
| Customer buys Hills brand cat food during Hills brand week | 1. Hills Brand: 15% off<br>2. Wet Cat Food Category: Buy 6 get 15% off | Highest applicable discount (both are 15%, system chooses one) |
| Customer is ESAAD member buying product on clearance | 1. ESAAD20: 20% off<br>2. CLEAR40: 40% off clearance item | CLEAR40 (40% is higher) |
| Online customer enters code for item already on EDLP | 1. EDLP price reduction<br>2. PET15 code: 15% off | PET15 code applies to already-reduced EDLP price |

**Important Notes:**
- ⚠️ Customers CANNOT combine two promo codes online
- ✅ Customers CAN use promo code on item with reduced price
- ✅ System automatically applies best discount for customer
- ✅ Only ONE discount recorded per transaction line

---

## Weekly/Monthly Patterns

### Weekly Sales Pattern (Impact on Campaign Timing)

| Day Type | Average Daily Revenue (Stores) | Multiplier | Campaign Strategy |
|----------|------------------------------|------------|-------------------|
| Weekday (Mon-Fri) | 80,000 - 90,000 AED | 1.0x | Regular campaigns |
| Weekend (Sat-Sun) | 140,000 - 150,000 AED | 1.7x | Peak campaign activity |

**Why This Matters for Baselines:**
- Must match day-of-week patterns when comparing campaign vs baseline
- Campaign running Tue-Mon must compare to baseline that's also Tue-Mon
- Otherwise results are skewed by weekend effect

---

### Monthly Sales Pattern (Payday Effect)

| Week of Month | Sales Level | Campaign Discount Level | Why |
|---------------|-------------|------------------------|-----|
| **Week 4 (Last week)** | HIGHEST | Most aggressive discounts | Customers have payday money |
| **Week 1 (First week)** | HIGHEST | Most aggressive discounts | Customers still have money from payday |
| Week 2 | Lower | Moderate discounts | Post-payday dip |
| Week 3 | Lower | Moderate discounts | Pre-payday tight budgets |

**Strategic Implication:**
- Best ROI campaigns run during Week 4 + Week 1
- Customers have more disposable income
- Higher discount spend justified by higher transaction values

---

## Margin Analysis by Discount Type

**From Week 13 (May) Campaign Performance:**

| Discount Type | Margin % | Interpretation |
|---------------|----------|----------------|
| **Codes (Loyalty)** | 32-43% | Healthy margin - these are loyal customers |
| **Thematic - Spend & Save** | 43% | Good margin - increases basket size |
| **Thematic - Category Mechanics** | 21-72% | Varies by category (food lower, accessories higher) |
| **Brand Campaigns** | 20-70% | Varies by brand positioning |
| **Clearance** | -37% to -183% | **Negative margin** - inventory clearance priority |
| **Organic** | 51% | Highest margin - no discount given |

**Key Insight:**
- Clearance is the ONLY discount type that runs at negative margin
- Purpose: Move inventory, not make profit
- All other campaigns maintain positive margins

---

## Example: May 2025 Campaign Mix (Stores)

**Total Revenue: 1,560,443 AED**

| Classification | Revenue (AED) | Revenue % | Margin (AED) | Margin % | Discount (AED) | Discount % |
|----------------|--------------|-----------|--------------|----------|----------------|------------|
| **Codes** | 406,012 | 26% | 145,562 | 36% | 91,793 | 18% |
| &nbsp;&nbsp;- ESAAD20 | 168,019 | 11% | 52,980 | 32% | 42,235 | 20% |
| &nbsp;&nbsp;- FAZAA15 | 158,014 | 10% | 62,599 | 40% | 28,195 | 15% |
| &nbsp;&nbsp;- EKTPS2024 | 66,911 | 4% | 28,866 | 43% | 11,949 | 15% |
| &nbsp;&nbsp;- Others | 13,068 | 1% | 1,117 | 9% | 9,414 | 72% |
| **Thematic Campaigns** | 378,258 | 24% | 167,430 | 44% | 67,124 | 15% |
| &nbsp;&nbsp;- Spend & Save 14% | 231,936 | 15% | 100,344 | 43% | 38,025 | 14% |
| &nbsp;&nbsp;- Everything Under 14 | 33,163 | 2% | 23,916 | 72% | 5,672 | 15% |
| &nbsp;&nbsp;- Wet Cat Food Mechanics | 30,352 | 2% | 6,467 | 21% | 5,126 | 14% |
| &nbsp;&nbsp;- Treats Mechanics | 28,464 | 2% | 11,379 | 40% | 6,991 | 20% |
| &nbsp;&nbsp;- Dog Beds & Trees | 22,114 | 1% | 11,470 | 52% | 5,523 | 20% |
| &nbsp;&nbsp;- Brand Campaigns | 32,229 | 2% | 13,854 | 43% | 5,787 | 15% |
| **Clearance** | 16,925 | 1% | 6,296 | -37% | 33,302 | 66% |
| **Organic** | 734,677 | 47% | 371,808 | 51% | 0 | 0% |

---

## Example: May 2025 Campaign Mix (Online)

**Total Revenue: 1,564,852 AED**

| Classification | Revenue (AED) | Revenue % | Margin (AED) | Margin % | Discount (AED) | Discount % |
|----------------|--------------|-----------|--------------|----------|----------------|------------|
| **Code** | 540,015 | 35% | 180,589 | 33% | 111,778 | 17% |
| &nbsp;&nbsp;- AN14 | 235,108 | 15% | 78,925 | 34% | 46,436 | 16% |
| &nbsp;&nbsp;- EKTPS2024 | 113,517 | 7% | 40,311 | 36% | 20,364 | 15% |
| &nbsp;&nbsp;- SHOP20 | 46,890 | 3% | 14,144 | 30% | 10,344 | 18% |
| &nbsp;&nbsp;- PET15 | 42,112 | 3% | 13,741 | 33% | 12,486 | 23% |
| &nbsp;&nbsp;- HBD20 | 41,929 | 3% | 12,293 | 29% | 10,067 | 19% |
| &nbsp;&nbsp;- Others | 60,459 | 4% | 21,175 | 35% | 11,981 | 20% |
| **Thematic Campaigns** | 343,100 | 22% | 126,863 | 37% | 56,957 | 14% |
| &nbsp;&nbsp;- Everyday Low Prices | 194,451 | 12% | 67,951 | 35% | 25,246 | 11% |
| &nbsp;&nbsp;- Wet Cat Food: Buy 6 Get 15% | 53,703 | 3% | 15,614 | 29% | 4,635 | 8% |
| &nbsp;&nbsp;- Under 14 AED | 31,622 | 2% | 15,478 | 49% | 19,059 | 38% |
| &nbsp;&nbsp;- Treats: Buy 5 Get 20% | 28,445 | 2% | 13,018 | 46% | 3,695 | 11% |
| &nbsp;&nbsp;- Brand Weeks | 32,623 | 2% | 13,501 | 41% | 4,094 | 11% |
| &nbsp;&nbsp;- Dog Beds & Trees | 2,257 | 0% | 1,301 | 58% | 564 | 20% |
| **Clearance** | 6,759 | 0% | 2,781 | -41% | 3,566 | 35% |
| **Organic** | 674,977 | 43% | 313,787 | 46% | 0 | 0% |

---

## Key Takeaways for Dashboard Requirements

### Must Track by Campaign Type:
1. **Revenue** - Total sales generated
2. **Margin** - Gross profit after discounts
3. **Discount** - Total discount amount given
4. **Orders** - Number of transactions
5. **Items** - Number of units sold
6. **Achievement %** - Actual vs Target
7. **Uplift** - Actual vs Baseline

### Data Requirements:

**For OFFLINE:**
- ✅ ERP Discount Ledger (has offer numbers)
- ✅ ERP Periodic Discount (maps to offer names)
- ✅ ERP Item Ledger (for margin calculation)
- ✅ Classification lookup table (offer name → campaign type)

**For ONLINE:**
- ✅ Shopify Orders discount codes
- ⚠️ Manual campaign mapping table (for price-based campaigns)
- ⚠️ SKU + Date Range = Campaign mapping

### Automation Priorities:

**High Priority (Can Automate):**
- Code-based campaigns (both channels)
- Spend & Save mechanics (offline)
- Category mechanics (offline)

**Medium Priority (Partial Automation):**
- Brand campaigns (if using codes)
- Category mechanics (online - requires validation)

**Low Priority (Manual Process):**
- Everyday Low Prices (online)
- Direct price-off campaigns (online)
- New campaign types not in classification table

---

**Document Created:** November 16, 2025  
**Source:** Campaign Performance Meeting Transcripts (June 26 & October 24, 2025)  
**For:** The Petshop Campaign Performance Dashboard Project

---
