with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_transfer_line_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
        document_no_,
        line_no_,
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        appl__to_item_entry,
        completely_received,
        completely_shipped,
        derived_from_line_no_,
        description,
        description_2,
        dimension_set_id,
        direct_transfer,
        gen__prod__posting_group,
        gross_weight,
        in_transit_code,
        inbound_whse__handling_time,
        inventory_posting_group,
        item_category_code,
        item_no_,
        net_weight,
        outbound_whse__handling_time,
        outstanding_qty___base_,
        outstanding_quantity,
        planning_flexibility,
        product_group_code,
        qty__in_transit,
        qty__in_transit__base_,
        qty__per_unit_of_measure,
        qty__received__base_,
        qty__shipped__base_,
        qty__to_receive,
        qty__to_receive__base_,
        qty__to_ship,
        qty__to_ship__base_,
        quantity,
        quantity__base_,
        quantity_received,
        quantity_shipped,
        receipt_date,
        shipment_date,
        shipping_agent_code,
        shipping_agent_service_code,
        shipping_time,
        shortcut_dimension_1_code,
        shortcut_dimension_2_code,
        status,
        timestamp,
        transfer_from_bin_code,
        transfer_from_code,
        transfer_to_bin_code,
        transfer_to_code,
        unit_of_measure,
        unit_of_measure_code,
        unit_volume,
        units_per_parcel,
        variant_code

    from source

)

select * from renamed
