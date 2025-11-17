# Team Repo Sync - Quick Guide

## Purpose
Share specific dbt files with junior team member via separate repo: `dbt-datago-thepetshop-team`

## Files Being Synced
- `models/2_int/5_item/int_items.sql`
- `models/2_int/5_item/int_items_2.sql`
- `models/3_fct/fact_commercial.sql`
- `models/3_fct/dim_items.sql`

## How to Sync Files

**Step 1:** Make changes to any of the files above

**Step 2:** Run this command:
```bash
./sync_to_team.sh
```

**Done!** Files are now in the team repo: https://github.com/datagotps/dbt-datago-thepetshop-team

---

## First Time Setup (One-Time Only)
If script won't run, make it executable:
```bash
chmod +x sync_to_team.sh
```

---

## Add More Files to Sync
Edit `sync_to_team.sh` and add files to the FILES array:
```bash
FILES=(
  "models/2_int/5_item/int_items.sql"
  "models/2_int/5_item/int_items_2.sql"
  "models/3_fct/fact_commercial.sql"
  "models/3_fct/dim_items.sql"
  "path/to/new/file.sql"  # Add here
)
```

---

**Questions?** Contact: anmar@8020datago.ai

