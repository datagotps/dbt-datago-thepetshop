with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_retail_product_group_5ecfc871_5d82_43f1_9c54_59685e82318d') }}

),

renamed as (

    select
        code,
        description,
        item_category_code,
        variant_framework_code,
        division_code,
        
        
        
        _systemid,
        allocation_rule_code,
        barcode_mask,
        buyer_group_code,
        buyer_id,
        def__item_distr__code,
        def__item_distr__type,
        default_base_uom,
        default_profit__,
        
        disable_dispense_printing,
        dispense_printer_group,
        
        item_error_check_code,
        item_template_code,
        last_date_modified,
        min_loc__prof__inventory,
        not_discountable,
        outbound_code,
        phys_invt_counting_period_code,
        pos_inventory_lookup,
        pos_menu_link,
        primary_key,
        profit_goal__,
        qty_not_in_decimal,
        replen__data_profile,
        replen__transfer_rule_code,
        shelf_label_description,
        suggested_qty__on_pos,
        timestamp,
        use_ean_standard_barc_,
        
        _fivetran_deleted,
        _fivetran_synced,

    from source

)

select * from renamed

--where code IN ('31024','31010','31011','31012','31113','31114')