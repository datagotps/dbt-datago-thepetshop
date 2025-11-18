# dbt-datago-thepetshop-team

**Team repository with selected dbt models - auto-synced from main repo**

## Overview

This repository is automatically synchronized with selected files from the main [dbt-datago-thepetshop](https://github.com/datagotps/dbt-datago-thepetshop) repository.

## Synced Files

The following dbt model files are automatically synced to this repository:

- `models/2_int/5_item/int_items.sql` - Intermediate items model
- `models/2_int/5_item/int_items_2.sql` - Intermediate items model (version 2)
- `models/3_fct/fact_commercial.sql` - Commercial fact table

## How It Works

A GitHub Actions workflow in the main repository automatically:
1. Monitors changes to the specified files
2. Copies only those files to this team repository
3. Maintains the same folder structure
4. Updates automatically when changes are pushed to the main branch

## Setup Instructions (For Repository Owner)

To enable automatic synchronization, you need to set up a GitHub Personal Access Token:

### Step 1: Create a Personal Access Token

1. Go to GitHub Settings → Developer settings → [Personal access tokens](https://github.com/settings/tokens/new)
2. Click "Generate new token" → "Generate new token (classic)"
3. Configure the token:
   - **Note**: `Team Repo Sync Token`
   - **Expiration**: Set to 90 days (or longer)
   - **Scopes**: Select the following:
     - ✅ `repo` (Full control of private repositories)
4. Click "Generate token"
5. **Important**: Copy the token immediately (you won't be able to see it again)

### Step 2: Add Token as a Repository Secret

1. Go to the main repository: [dbt-datago-thepetshop](https://github.com/datagotps/dbt-datago-thepetshop)
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Set:
   - **Name**: `TEAM_REPO_TOKEN`
   - **Value**: Paste the token you copied
5. Click **Add secret**

### Step 3: Test the Workflow

1. Go to the main repository
2. Navigate to **Actions** tab
3. Select "Sync Selected Files to Team Repo" workflow
4. Click "Run workflow" → "Run workflow" button
5. Wait for the workflow to complete
6. Check this repository to confirm the files have been synced

## Usage

### For Team Members

- This repository contains only the selected dbt models
- You can clone this repository to work on these specific models
- Changes made here will NOT sync back to the main repository
- To contribute changes, coordinate with the repository owner

### Workflow Triggers

The sync happens automatically when:
- Any of the synced files are modified in the main repository
- You can also manually trigger the sync from the Actions tab

## Folder Structure

```
dbt-datago-thepetshop-team/
└── models/
    ├── 2_int/
    │   └── 5_item/
    │       ├── int_items.sql
    │       └── int_items_2.sql
    └── 3_fct/
        └── fact_commercial.sql
```

## Contact

For questions or access requests, contact: anmar@8020datago.ai

---

*This repository is maintained by DataGo (8020datago.ai)*
