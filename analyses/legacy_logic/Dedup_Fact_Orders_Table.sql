with
    react as (
        select phonenumber, max(active_tag) active_tag
        from fact.fact_reactivation_master
        where
            snapshot_date
            = (select max(snapshot_date) from fact.fact_reactivation_master)
        group by 1
    ),
    occ_order as (
        select fot.* from fact.fact_orders_table fot where data_source = "OCC"
    ),
    erp as (select fot.* from fact.fact_orders_table fot where data_source = "ERP"),
    erp_only as (
        select *
        from erp
        where not exists (select 1 from occ_order where occ_order.orderno = erp.orderno)

    )
select x.*, rt.active_tag
from
    (
        select *
        from occ_order
        union all
        select *
        from erp_only
    ) x
left join react rt on x.phonenumber = rt.phonenumber
