# Development Data Filtering - Implementation Summary

## Problem Solved
Previously, you had to manually edit each model to comment/uncomment WHERE clauses for development filtering. This was:
- ❌ Time-consuming (editing 5+ models)
- ❌ Error-prone (forgetting to uncomment before production)
- ❌ Inconsistent (different date ranges across models)

## Solution Implemented
A dbt macro-based approach using variables that allows you to toggle filtering with a single command.

## What Was Created

### 1. Core Macro
**File**: `macros/dev_date_filter.sql`
- Reusable macro that generates date filtering logic
- Controlled by `dev_mode` variable
- Supports custom date ranges per model

### 2. Configuration
**File**: `dbt_project.yml` (updated)
- Added `dev_mode: false` variable (default = production)
- Added `dev_date_ranges` with default date ranges
- Centralized configuration

### 3. Updated Models
All models now use the macro instead of manual WHERE clauses:

| Model | Date Column | Status |
|-------|-------------|--------|
| `dim_customers.sql` | `customer_acquisition_date` | ✅ Updated |
| `fact_commercial.sql` | `posting_date` | ✅ Updated |
| `fact_orders.sql` | `order_date` | ✅ Updated |
| `fct_daily_transactions.sql` | `transaction_date` | ✅ Updated |
| `fct_procurement.sql` | `order_date` | ✅ Updated (custom ranges) |

### 4. Documentation
- `macros/README_dev_filtering.md` - Detailed documentation
- `DEV_MODE_QUICK_REFERENCE.md` - Quick command reference
- `scripts/dbt_dev_helpers.sh` - Shell aliases for convenience

## How to Use

### Development (Filtered Data)
```bash
# Single command to run with filtered data
dbt run --vars 'dev_mode: true'

# Or use the helper alias (after sourcing the script)
source scripts/dbt_dev_helpers.sh
dbt-dev
```

### Production (Full Data)
```bash
# Default behavior - no changes needed
dbt run

# Or use the helper alias
dbt-prod
```

### Specific Models
```bash
# Dev mode for one model
dbt run --select fact_orders --vars 'dev_mode: true'

# Dev mode for model and downstream
dbt run --select fact_orders+ --vars 'dev_mode: true'
```

## Verification

✅ **Tested**: Compiled models with dev_mode true/false
✅ **Confirmed**: Date filters apply correctly when enabled
✅ **Confirmed**: No filters when disabled (production mode)

### Example Compiled Output

**With dev_mode: true**:
```sql
WHERE sales_channel in ('Online','Shop')
AND (
    order_date BETWEEN '2025-01-01' AND '2025-09-30'
    OR order_date BETWEEN '2024-12-01' AND '2024-12-31'
    OR order_date BETWEEN '2024-01-01' AND '2024-01-31'
)
```

**With dev_mode: false** (default):
```sql
WHERE sales_channel in ('Online','Shop')
```

## Benefits

### Time Savings
- **Before**: Edit 5 models × 2 times (comment/uncomment) = 10 manual edits per dev cycle
- **After**: 1 command to toggle mode

### Safety
- No risk of forgetting to uncomment filters
- Default is production mode (safe)
- Version controlled configuration

### Flexibility
- Override date ranges per model
- Override date ranges per run
- Works with all dbt commands (run, build, test, compile)

### Consistency
- Same date ranges across all models
- Centralized configuration
- Easy to update globally

## Advanced Usage

### Custom Date Ranges at Runtime
```bash
dbt run --vars '{"dev_mode": true, "dev_date_ranges": [{"start": "2025-01-01", "end": "2025-03-31"}]}'
```

### Profile-Specific Configuration
Add to `profiles.yml`:
```yaml
thepetshop:
  target: dev
  outputs:
    dev:
      type: bigquery
      vars:
        dev_mode: true  # Always on for dev target
    prod:
      type: bigquery
      vars:
        dev_mode: false  # Always off for prod target
```

### Shell Aliases
```bash
# Source the helper script
source scripts/dbt_dev_helpers.sh

# Use convenient aliases
dbt-dev                      # Run all in dev mode
dbt_dev_select fact_orders   # Run one model in dev mode
dbt-prod                     # Run all in prod mode
```

## Next Steps

1. **Test the implementation**:
   ```bash
   dbt run --select fact_orders --vars 'dev_mode: true'
   ```

2. **Verify the output**:
   - Check row counts are reduced
   - Verify date ranges are correct

3. **Add to other models** (if needed):
   ```sql
   WHERE your_conditions
   {{ dev_date_filter('your_date_column') }}
   ```

4. **Optional**: Set up shell aliases for your workflow

## Maintenance

### Updating Date Ranges
Edit `dbt_project.yml`:
```yaml
vars:
  dev_date_ranges:
    - {start: '2025-01-01', end: '2025-12-31'}  # Update as needed
```

### Adding to New Models
Simply add to your WHERE clause:
```sql
{{ dev_date_filter('date_column_name') }}
```

## Support

- See `macros/README_dev_filtering.md` for detailed documentation
- See `DEV_MODE_QUICK_REFERENCE.md` for quick commands
- Run `dbt_dev_help` (after sourcing helpers) for command reference
