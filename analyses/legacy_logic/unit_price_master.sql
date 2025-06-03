with
    cte1 as (
        select item_no_, starting_date, unit_price, ymdkey
        from `Fact.Fact_Unit_Price`
        where starting_date = (select max(starting_date) from `Fact.Fact_Unit_Price`)
    ),
    cte2 as (
        select distinct item_no_
        from `sql_erp_prod_dbo.petshop_sales_price_437dbf0e_84ff_417a_965d_ed2bb9650972`
        where sales_code = 'AED' and item_no_ like '%-%'
    ),
    cte3 as (
        select item_no_, starting_date, unit_price
        from
            `sql_erp_prod_dbo.petshop_sales_price_437dbf0e_84ff_417a_965d_ed2bb9650972`
            as t
        where
            starting_date = date_sub(current_date(), interval 1 day)
            and sales_code = 'AED'
    )
select
    cte2.item_no_,
    date_sub(current_date(), interval 1 day) as starting_date,
    case
        when cte3.unit_price is not null then cte3.unit_price else cte1.unit_price
    end as unit_price,
    concat(
        cast(extract(year from date_sub(current_date(), interval 1 day)) as string),
        format_date('%m%d', date_sub(current_date(), interval 1 day)),
        cte2.item_no_
    ) as ymdkey
from cte2
left join cte1 on cte2.item_no_ = cte1.item_no_
left join cte3 on cte2.item_no_ = cte3.item_no_
