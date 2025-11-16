# 🚀 Dev Mode Quick Reference

## Quick Commands

```bash
# Development (filtered data)
dbt run --vars 'dev_mode: true'

# Production (full data)
dbt run
```

## What Happens?

### Dev Mode OFF (default)
- ✅ Full data processing
- ✅ All historical records
- ✅ Production-ready

### Dev Mode ON
- ⚡ Filtered to specific date ranges
- ⚡ Faster development cycles
- ⚡ Reduced compute costs

## Date Ranges (Default)

When `dev_mode: true`, models filter to:
- **2025-01-01 to 2025-09-30** (Jan-Sep 2025)
- **2024-12-01 to 2024-12-31** (Dec 2024)
- **2024-01-01 to 2024-01-31** (Jan 2024)

## Models Using Dev Mode

- ✅ `dim_customers` (customer_acquisition_date)
- ✅ `fact_commercial` (posting_date)
- ✅ `fact_orders` (order_date)
- ✅ `fct_daily_transactions` (transaction_date)
- ✅ `fct_procurement` (order_date - custom ranges)

## Common Workflows

### Testing Changes
```bash
# 1. Make your code changes
# 2. Run with filtered data
dbt run --select my_model --vars 'dev_mode: true'
# 3. Verify results
# 4. Run full data
dbt run --select my_model
```

### Building Multiple Models
```bash
# Dev mode for faster iteration
dbt run --select fact_orders+ --vars 'dev_mode: true'

# Production run when ready
dbt run --select fact_orders+
```

### Full Refresh in Dev Mode
```bash
dbt run --full-refresh --vars 'dev_mode: true'
```

## Need Help?

See `macros/README_dev_filtering.md` for detailed documentation.
