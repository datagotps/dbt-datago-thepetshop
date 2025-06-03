with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_transfer_header_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
        no_,
        transfer_from_code,
        transfer_to_code,
        shipment_date,

        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        area,
        assigned_user_id,
        dimension_set_id,
        direct_transfer,
        entry_exit_point,
        external_document_no_,
        in_transit_code,
        inbound_whse__handling_time,
        last_receipt_no_,
        last_shipment_no_,
        no__series,
        outbound_whse__handling_time,
        posting_date,
        posting_from_whse__ref_,
        receipt_date,
        
        shipment_method_code,
        shipping_advice,
        shipping_agent_code,
        shipping_agent_service_code,
        shipping_time,
        shortcut_dimension_1_code,
        shortcut_dimension_2_code,
        status,
        timestamp,
        transaction_specification,
        transaction_type,
        transfer_from_address,
        transfer_from_address_2,
        transfer_from_city,
        
        transfer_from_contact,
        transfer_from_county,
        transfer_from_name,
        transfer_from_name_2,
        transfer_from_post_code,
        transfer_to_address,
        transfer_to_address_2,
        transfer_to_city,
        transfer_to_contact,
        transfer_to_county,
        transfer_to_name,
        transfer_to_name_2,
        transfer_to_post_code,
        transport_method,
        trsf__from_country_region_code,
        trsf__to_country_region_code

    from source

)

select * from renamed
