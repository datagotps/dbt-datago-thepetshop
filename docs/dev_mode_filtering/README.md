# Development Data Filtering

## Overview
This macro provides a consistent way to filter data during development across all models, eliminating the need to manually comment/uncomment WHERE clauses.

## How It Works

### 1. The Macro: `dev_date_filter`
Located in `macros/dev_date_filter.sql`, this macro:
- Checks if `dev_mode` variable is enabled
- Applies date range filters only when in dev mode
- Supports custom date ranges per model or uses global defaults

### 2. Configuration
In `dbt_project.yml`:
```yaml
vars:
  dev_mode: false  # Toggle development mode
  dev_date_ranges:  # Default date ranges
    - {start: '2025-01-01', end: '2025-09-30'}
    - {start: '2024-12-01', end: '2024-12-31'}
    - {start: '2024-01-01', end: '2024-01-31'}
```

## Usage

### Basic Usage (Uses Default Date Ranges)
In your model SQL:
```sql
FROM {{ ref('my_table') }}
WHERE some_condition = true
{{ dev_date_filter('order_date') }}
```

### Custom Date Ranges Per Model
For models needing different date ranges (like `fct_procurement`):
```sql
FROM {{ ref('my_table') }}
WHERE 1=1
{{ dev_date_filter('order_date', [
    {'start': '2024-04-01', 'end': '2024-04-30'},
    {'start': '2025-03-01', 'end': '2025-03-31'}
]) }}
```

## Running Models

### Development Mode (Filtered Data)
```bash
# Run with dev filtering enabled
dbt run --vars 'dev_mode: true'

# Run specific model
dbt run --select fact_orders --vars 'dev_mode: true'

# Run multiple models
dbt run --select fact_orders fact_commercial --vars 'dev_mode: true'
```

### Production Mode (Full Data)
```bash
# Default behavior - no filtering
dbt run

# Or explicitly disable dev mode
dbt run --vars 'dev_mode: false'
```

### Override Date Ranges at Runtime
```bash
# Use custom date ranges for this run
dbt run --vars '{"dev_mode": true, "dev_date_ranges": [{"start": "2025-01-01", "end": "2025-03-31"}]}'
```

## Models Updated

The following models now use this macro:

| Model | Date Column | Date Ranges |
|-------|-------------|-------------|
| `dim_customers` | `customer_acquisition_date` | Default |
| `fact_commercial` | `posting_date` | Default |
| `fact_orders` | `order_date` | Default |
| `fct_daily_transactions` | `transaction_date` | Default |
| `fct_procurement` | `order_date` | Custom (Apr 2024, Mar-Apr 2025) |

## Benefits

✅ **No More Manual Editing**: Toggle dev mode with a single command
✅ **Consistent Filtering**: Same date ranges across all models
✅ **Flexible**: Override per model or per run
✅ **Safe**: Default is production mode (no filtering)
✅ **Version Controlled**: Date ranges are in `dbt_project.yml`

## Tips

1. **Set Default in dbt_project.yml**: Keep `dev_mode: false` in the file to prevent accidental filtered production runs

2. **Use Aliases**: Create shell aliases for convenience:
   ```bash
   alias dbt-dev='dbt run --vars "dev_mode: true"'
   alias dbt-prod='dbt run'
   ```

3. **Profile-Specific Variables**: You can set dev_mode in your `profiles.yml` per target:
   ```yaml
   thepetshop:
     target: dev
     outputs:
       dev:
         type: bigquery
         # ... other configs
         vars:
           dev_mode: true
       prod:
         type: bigquery
         # ... other configs
         vars:
           dev_mode: false
   ```

## Troubleshooting

**Q: My model still returns full data even with dev_mode: true**
- Check that the macro is called in your model's WHERE clause
- Verify the date column name matches exactly
- Run `dbt compile` and check the compiled SQL in `target/compiled/`

**Q: I want different date ranges for a specific model**
- Pass custom ranges directly to the macro (see Custom Date Ranges example above)

**Q: Can I use this with incremental models?**
- Yes! The macro works with any materialization type
- For incremental models, the filter applies to the full refresh and incremental logic

## Example Compiled SQL

When `dev_mode: true`, this model:
```sql
FROM {{ ref('int_orders') }}
WHERE sales_channel = 'Online'
{{ dev_date_filter('order_date') }}
```

Compiles to:
```sql
FROM analytics.int_orders
WHERE sales_channel = 'Online'
AND (
    order_date BETWEEN '2025-01-01' AND '2025-09-30'
    OR order_date BETWEEN '2024-12-01' AND '2024-12-31'
    OR order_date BETWEEN '2024-01-01' AND '2024-01-31'
)
```

When `dev_mode: false` (default):
```sql
FROM analytics.int_orders
WHERE sales_channel = 'Online'
```
