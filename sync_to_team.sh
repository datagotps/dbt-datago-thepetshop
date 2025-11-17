#!/bin/bash
# Simple manual sync script - run this when you want to sync files to team repo

echo "🔄 Syncing files to team repo..."

# Files to sync
FILES=(
  "models/2_int/5_item/int_items.sql"
  "models/2_int/5_item/int_items_2.sql"
  "models/3_fct/fact_commercial.sql"
  "models/3_fct/dim_items.sql"
)

# Clone team repo if not exists
if [ ! -d "../dbt-datago-thepetshop-team" ]; then
  cd ..
  git clone https://github.com/datagotps/dbt-datago-thepetshop-team.git
  cd dbt-datago-thepetshop
fi

# Copy files
for file in "${FILES[@]}"; do
  mkdir -p "../dbt-datago-thepetshop-team/$(dirname "$file")"
  cp "$file" "../dbt-datago-thepetshop-team/$file"
  echo "✓ Copied $file"
done

# Commit and push
cd ../dbt-datago-thepetshop-team
git add .
git commit -m "Manual sync: $(date '+%Y-%m-%d %H:%M')"
git push origin main

echo "✅ Done! Files synced to team repo"

