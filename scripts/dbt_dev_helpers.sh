#!/bin/bash
# dbt Development Mode Helper Scripts
# Source this file to use the aliases: source scripts/dbt_dev_helpers.sh

# Color codes for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Development mode (filtered data)
alias dbt-dev='echo -e "${BLUE}Running in DEV MODE (filtered data)${NC}" && dbt run --vars "dev_mode: true"'
alias dbt-dev-build='echo -e "${BLUE}Building in DEV MODE (filtered data)${NC}" && dbt build --vars "dev_mode: true"'
alias dbt-dev-test='echo -e "${BLUE}Testing in DEV MODE (filtered data)${NC}" && dbt test --vars "dev_mode: true"'

# Production mode (full data)
alias dbt-prod='echo -e "${GREEN}Running in PROD MODE (full data)${NC}" && dbt run'
alias dbt-prod-build='echo -e "${GREEN}Building in PROD MODE (full data)${NC}" && dbt build'
alias dbt-prod-test='echo -e "${GREEN}Testing in PROD MODE (full data)${NC}" && dbt test'

# Compile and check
alias dbt-compile-dev='dbt compile --vars "dev_mode: true"'
alias dbt-compile-prod='dbt compile --vars "dev_mode: false"'

# Functions for more complex operations
dbt_dev_select() {
    echo -e "${BLUE}Running $1 in DEV MODE (filtered data)${NC}"
    dbt run --select "$1" --vars "dev_mode: true"
}

dbt_prod_select() {
    echo -e "${GREEN}Running $1 in PROD MODE (full data)${NC}"
    dbt run --select "$1"
}

# Show help
dbt_dev_help() {
    echo "=== dbt Development Mode Helpers ==="
    echo ""
    echo "Aliases:"
    echo "  dbt-dev              - Run all models with filtered data"
    echo "  dbt-dev-build        - Build all models with filtered data"
    echo "  dbt-dev-test         - Test all models with filtered data"
    echo "  dbt-prod             - Run all models with full data"
    echo "  dbt-prod-build       - Build all models with full data"
    echo "  dbt-prod-test        - Test all models with full data"
    echo "  dbt-compile-dev      - Compile in dev mode"
    echo "  dbt-compile-prod     - Compile in prod mode"
    echo ""
    echo "Functions:"
    echo "  dbt_dev_select <model>   - Run specific model in dev mode"
    echo "  dbt_prod_select <model>  - Run specific model in prod mode"
    echo ""
    echo "Examples:"
    echo "  dbt-dev                          # Run all with filtered data"
    echo "  dbt_dev_select fact_orders       # Run one model with filtered data"
    echo "  dbt_dev_select 'fact_orders+'    # Run model and downstream"
    echo "  dbt-prod                         # Run all with full data"
    echo ""
}

# Show help on load
echo -e "${GREEN}dbt Development Mode helpers loaded!${NC}"
echo "Type 'dbt_dev_help' for usage information"
