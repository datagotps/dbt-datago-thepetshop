# Development Mode Filtering Documentation

This folder contains all documentation related to the development mode filtering feature.

## 📚 Documentation Files

### 1. [README.md](README.md)
**Comprehensive Guide**
- Detailed explanation of how the macro works
- Configuration options
- Usage examples
- Troubleshooting guide
- Best practices

👉 **Start here** for complete understanding

---

### 2. [quick_reference.md](quick_reference.md)
**Quick Commands**
- Common commands at a glance
- Quick workflows
- Date ranges reference
- Models list

👉 **Use this** for day-to-day work

---

### 3. [implementation_summary.md](implementation_summary.md)
**Implementation Details**
- What was created
- Files modified
- Verification steps
- Benefits and time savings

👉 **Review this** to understand what changed

---

### 4. [workflow_diagram.md](workflow_diagram.md)
**Visual Workflows**
- Before/After comparison
- Architecture diagrams
- Command flow visualization
- Data flow comparison

👉 **See this** for visual understanding

---

## 🚀 Quick Start

### Development Mode (Filtered Data)
```bash
dbt run --vars 'dev_mode: true'
```

### Production Mode (Full Data)
```bash
dbt run
```

---

## 📁 Related Files

### Code Files
- `macros/dev_date_filter.sql` - The macro implementation
- `dbt_project.yml` - Configuration variables

### Helper Scripts
- `scripts/dbt_dev_helpers.sh` - Shell aliases (local terminal only)

### Models Using This Feature
- `models/3_fct/dim_customers.sql`
- `models/3_fct/fact_commercial.sql`
- `models/3_fct/fact_orders.sql`
- `models/3_fct/fct_daily_transactions.sql`
- `models/3_fct/fct_procurement.sql`

---

## 💡 Need Help?

1. **Quick commands**: See [quick_reference.md](quick_reference.md)
2. **Detailed guide**: See [README.md](README.md)
3. **Visual explanation**: See [workflow_diagram.md](workflow_diagram.md)
4. **What changed**: See [implementation_summary.md](implementation_summary.md)
