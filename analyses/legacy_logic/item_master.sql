select
    *,
    case
        when olditemno is null or olditemno = '' then itemno else olditemno
    end as item_key
from
    (
        select
            a.no_ as itemno,
            a.no__2 as olditemno,
            a.vendor_no_ as vendorno,
            a.vendor_item_no_ as vendoritemno,
            c.original_vendor_item_no_ as originalvendoritemno,
            e.ps_classification as ps_classification_code,
            a.blocked,
            a.description as productname,
            e.brand as brand,
            a.inventory_posting_group as division,
            b.description as item_category,
            (
                case
                    when c.retail_product_code = '21035' then 'Supplements' else d.name
                end
            ) as item_sub_category,
            e.grade as grade,
            a.unit_cost,
            a.unit_price,
            a.base_unit_of_measure as base_unit_of_measure,
            e.packet_unit_of_measure as packet_unit_of_measure,
            a.safety_stock_quantity,
            a.tariff_no_ as tariff_no,
            f.description as tariff_description
        from `sql_erp_prod_dbo.petshop_item_437dbf0e_84ff_417a_965d_ed2bb9650972` a
        left join
            `sql_erp_prod_dbo.petshop_item_category_437dbf0e_84ff_417a_965d_ed2bb9650972` b
            on a.item_category_code = b.code
        left join
            `sql_erp_prod_dbo.petshop_item_5ecfc871_5d82_43f1_9c54_59685e82318d` c
            on a.no_ = c.no_
        left join
            `sql_erp_prod_dbo.petshop_dimension_value_437dbf0e_84ff_417a_965d_ed2bb9650972` d
            on c.retail_product_code = d.code
        left join
            `sql_erp_prod_dbo.petshop_item_c91094c2_db03_49d2_8cb9_95c179ecbf9d` e
            on a.no_ = e.no_
        left join
            (
                select no_, description
                from
                    `sql_erp_prod_dbo.petshop_tariff_number_437dbf0e_84ff_417a_965d_ed2bb9650972`
            ) f
            on a.tariff_no_ = f.no_
        where e.varient_item = 0
        union all
        select
            a.no as itemno,
            null as olditemno,
            vendor_no as vendorno,
            vendor_item_no as vendoritemno,
            '' as originalvendoritemno,
            ps_classification_code as ps_classification_code,
            blocked as blocked,
            dwi_product_name as productname,
            dwi_brand as brand,
            null as division,
            case
                when item_category_code in ('DOG FOOD', 'DOG-FOOD')
                then 'Dog Food'
                when item_category_code in ('CAT FOOD')
                then 'Cat Food'
                when item_category_code = 'REPTILES'
                then 'REPTILE'
                when item_category_code = 'TREAT'
                then 'Treats'
                else item_category_code
            end as itemcategory,
            product_group_code as itemsubcategory,
            null as grade,
            unit_cost as unitcost,
            unit_price as unitprice,
            base_unit_of_measure as baseunitofmeasure,
            '' as packetunitofmeasure,
            null as safetystockquantity,
            cast(tariff_no as string) as tariffno,  -- Convert TariffNo to STRING '' AS TariffDescription FROM `Archive_Old_ERP_Data.nav_item_list` a LEFT JOIN ( SELECT DISTINCT no__2 AS Item_No FROM `sql_erp_prod_dbo.petshop_item_437dbf0e_84ff_417a_965d_ed2bb9650972` ) G ON a.no = Item_No WHERE Item_No IS NULL );
