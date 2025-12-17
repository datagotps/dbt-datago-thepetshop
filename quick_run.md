# Quick Commands

```bash
dbt run --select dim_date fct_occ_order_items dim_customers fact_orders fact_commercial fct_daily_transactions fct_procurement --vars 'dev_mode: true'
```


---dbt parse to refresh the project manifest:

git subtree pull --prefix=team_work team main --squash
git subtree push --prefix=team_work team main

1. Pull updates from the team repo into your repo

Purpose: Fetch the latest changes from the main branch of the remote repo called team (e.g. your team’s GitHub repo) and merge them into the team_work folder in your current repo.

2. Push your updates from the staging folder back to the team repo
Purpose: Send changes you’ve made in the team_work folder in your repo up to the main branch of the team’s remote repo.

