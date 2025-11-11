# The Pet Shop - dbt Project Documentation

This folder contains technical documentation for major features and enhancements to the dbt data models.

---

## 📚 Available Documentation

### Development Tools

1. **[Development Mode Filtering](./dev_mode_filtering/)**
   - Toggle data filtering for faster development cycles
   - Filter models by date ranges during development
   - **Use Case**: Speed up development, reduce compute costs
   - **Date Added**: November 2025
   - **Quick Start**: `dbt run --vars 'dev_mode: true'`

### Customer Segmentation Features

2. **[Offer Seeker Segmentation](./offer_seeker_segmentation.md)**
   - Identifies customers who frequently use discounts and offers
   - Segments: Offer Seeker, Occasional Offer User, Non-Offer User
   - **Use Case**: Targeted promotional campaigns and discount optimization
   - **Date Added**: November 2025

3. **[Pet Ownership Categorization](./pet_ownership_categorization.md)**
   - Categorizes customers by pet type ownership (Dog, Cat, Fish, Bird, Small Pet, Reptile)
   - Identifies multi-pet owners for cross-sell opportunities
   - **Use Case**: Pet-specific marketing campaigns and product recommendations
   - **Date Added**: November 2025

---

## 🎯 Quick Reference

### Development Mode Filtering
**Quick Commands**:
```bash
# Development (filtered data)
dbt run --vars 'dev_mode: true'

# Production (full data)
dbt run
```

**Documentation**: See [dev_mode_filtering/](./dev_mode_filtering/) folder

---

### Offer Seeker Segmentation
**Key Fields**:
- `offer_seeker_segment` - Main segmentation field
- `orders_with_discount_count` - Number of orders with discounts
- `total_discount_amount` - Total AED saved

**Quick Query**:
```sql
SELECT offer_seeker_segment, COUNT(*) 
FROM dim_customers 
GROUP BY offer_seeker_segment
```

---

### Pet Ownership Categorization
**Key Fields**:
- `pet_owner_profile` - Main categorization field (Dog Owner, Cat Owner, Multi-Pet Owner, etc.)
- `primary_pet_type` - Pet type with highest revenue
- `is_dog_owner`, `is_cat_owner`, etc. - Binary flags

**Quick Query**:
```sql
SELECT pet_owner_profile, COUNT(*) 
FROM dim_customers 
GROUP BY pet_owner_profile
```

---

## 📊 Data Models Overview

### Core Models
- **`dim_customers`** - Customer dimension table with all segmentation fields
- **`int_customers`** - Intermediate customer model with transformation logic
- **`int_order_lines`** - Source for transaction-level data
- **`dim_items`** - Product dimension with category hierarchy

### Model Hierarchy
```
int_order_lines
    ↓
int_customers (transformations & aggregations)
    ↓
dim_customers (final dimension for BI/CRM)
```

---

## 🏗️ Product Hierarchy

The Pet Shop uses a 5-level product hierarchy:

```
Level 1: DIVISION (Pet Type)
    ↓
Level 2: ITEM_CATEGORY (Product Type)
    ↓
Level 3: ITEM_SUBCATEGORY (Product Group)
    ↓
Level 4: ITEM_BRAND (Brand Name)
    ↓
Level 5: ITEM_ID (Individual SKU)
```

**Divisions**: DOG, CAT, FISH, BIRD, SMALL PET, REPTILE, SERVICE, HUMAN, AQUA, NON FOOD, FOOD

---

## 🚀 Deployment Guide

### Running Models
```bash
# Run all models
dbt run

# Run specific models
dbt run --models int_customers dim_customers

# Run with full refresh
dbt run --models int_customers --full-refresh
```

### Testing
```bash
# Run all tests
dbt test

# Test specific model
dbt test --models dim_customers
```

### Documentation
```bash
# Generate dbt docs
dbt docs generate

# Serve docs locally
dbt docs serve
```

---

## 📝 Documentation Standards

When adding new documentation:

1. **File Naming**: Use snake_case (e.g., `feature_name.md`)
2. **Structure**: Follow the template in existing docs
3. **Include**:
   - Business requirement
   - Technical implementation
   - SQL examples
   - Validation queries
   - Files modified
4. **Update**: This README with links to new docs

---

## 🔗 Related Resources

- **dbt Project**: `/models/`
- **SQL Queries**: See individual documentation files
- **Data Dictionary**: Run `dbt docs generate` and `dbt docs serve`
- **Source Data**: BigQuery project `tps-data-386515`

---

## 👥 Contact

For questions or support:
- **Data Engineering Team**: [Contact Info]
- **dbt Project Owner**: [Name]
- **CRM Team**: [Contact Info]

---

## 📅 Change Log

| Date | Feature | Documentation |
|------|---------|---------------|
| Nov 11, 2025 | Development Mode Filtering | [Link](./dev_mode_filtering/) |
| Nov 2025 | Offer Seeker Segmentation | [Link](./offer_seeker_segmentation.md) |
| Nov 2025 | Pet Ownership Categorization | [Link](./pet_ownership_categorization.md) |

---

**Last Updated**: November 11, 2025  
**Version**: 1.1
