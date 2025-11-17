# Team Repository Sync - Manual Solution

## Overview
This solution allows you to sync specific dbt model files from the main repository to the team repository (`dbt-datago-thepetshop-team`) for your junior team member.

## Files Being Synced
The following 4 files are synced to the team repo:
1. `models/2_int/5_item/int_items.sql`
2. `models/2_int/5_item/int_items_2.sql`
3. `models/3_fct/fact_commercial.sql`
4. `models/3_fct/dim_items.sql`

## How to Use

### Simple Method - Run the Script

Whenever you make changes to any of the 4 files above and want to share them with your team member:

1. Open your terminal
2. Navigate to the project directory:
   ```bash
   cd "/Users/anmarabbas/Claude MCP/dbt-datago-thepetshop"
   ```
3. Run the sync script:
   ```bash
   ./sync_to_team.sh
   ```

That's it! The script will:
- ✅ Clone the team repo (first time only)
- ✅ Copy the 4 files to the team repo
- ✅ Commit the changes
- ✅ Push to GitHub

### What the Script Does

```bash
# 1. Checks if team repo exists locally, clones if needed
# 2. Copies the 4 specified files
# 3. Creates a commit with timestamp
# 4. Pushes changes to GitHub
```

### Verification

After running the script, check that files were synced:
- Visit: https://github.com/datagotps/dbt-datago-thepetshop-team
- You should see your latest changes

## Troubleshooting

### "Permission denied" when running script
Run this command to make it executable:
```bash
chmod +x sync_to_team.sh
```

### Git push requires authentication
Make sure you're logged into GitHub on your Mac. If needed:
```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

### Want to sync different files?
Edit `sync_to_team.sh` and modify the FILES array:
```bash
FILES=(
  "models/2_int/5_item/int_items.sql"
  "models/2_int/5_item/int_items_2.sql"
  "models/3_fct/fact_commercial.sql"
  "models/3_fct/dim_items.sql"
  # Add more files here
)
```

## Benefits of This Approach

✅ **Simple** - Just run one command
✅ **Reliable** - No GitHub Actions complexity
✅ **Fast** - Syncs in seconds
✅ **Controllable** - You decide when to sync
✅ **No tokens needed** - Uses your existing Git authentication

## Team Member Access

Your junior team member can access the synced files at:
- Repository: https://github.com/datagotps/dbt-datago-thepetshop-team
- They can clone it with: `git clone https://github.com/datagotps/dbt-datago-thepetshop-team.git`

---

**Created:** November 17, 2025
**Branch:** dev_anmar → team repo main branch

