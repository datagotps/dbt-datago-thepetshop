-- =====================================================
-- stg_erp_location: Staging model for ERP Location master
-- Source: PetShop ERP Business Central - Location table
-- Contains store/location information from the ERP system
-- =====================================================

WITH 

source AS (
    SELECT * FROM {{ source('sql_erp_prod_dbo', 'petshop_location_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}
),

renamed AS (
    SELECT
        -- Primary Key
        code AS location_code,
        
        -- Store Information
        store_type,
        store_hour,
        store_services,
        store_start_date,
        store_end_date,
        store_image_url,
        store_error_msg,
        store_synced_at,
        store_sync_with_web,
        
        -- Location Information
        state AS emirate,
        warehouse_code,
        site_id,
        default_loc__dimension_value,
        default_cost_center,
        
        -- Capabilities
        disable AS is_disabled,
        pick_up AS is_pickup_enabled,
        web_store AS is_web_store,
        web_location AS is_web_location,
        location_sync_with_web,
        location_synced_at,
        location_error_msg,
        sample_location AS is_sample_location,
        
        -- Shopify Integration
        shopify_location_id,
        shopify_b2b_location_id,
        
        -- Priority & Sorting
        priority,
        replenishmentpriority AS replenishment_priority,
        putawaypriority AS putaway_priority,
        stock_update,
        
        -- Bin Codes
        pos_sale_bin_code,
        pos_return_bin_code,
        default_pos_putpick_bin,
        sale_ship_bin_code,
        sales_receipt_bin,
        sales_receipt_fail_bin,
        sales_receipt_refurbish_bin,
        purchase_ship_bin_code,
        purchase_receipt_fail_bin,
        transfer_ship_bin_code,
        to_receipt_bin,
        to_receipt_fail_bin,
        drop_ship_bin,
        auto_pna_transfer_bin_code,
        manifest_bin_code,
        staging_bin,
        sample_bin,
        expired_bin,
        excess_bin,
        cc_stage_bin,
        bin_type_code,
        
        -- Number Series
        picknoseries,
        putnoseries,
        purch_put_away_nos,
        purch__return_pick_nos,
        to_pick_nos,
        to_put_away_nos,
        passed_nos,
        failed_nos,
        b2c_sales_pick_nos,
        b2c_sales_return_put_away_nos,
        b2b_sales_pick_nos,
        b2b_sales_return_put_away_nos,
        item_jounnal_no__series,
        
        -- Operational Settings
        allowed_pick_lines,
        allowed_put_away_line,
        minimum_expiry_period,
        assembly_rm_tolerance,
        default_pick_device,
        b2b_device_id,
        pna_count,
        auto_pna_transfer,
        auto_inventory_pos_posting,
        reason_mandatory,
        gate_entry_manadatory,
        sample___tester_not_allowed,
        
        -- Operations Type
        purch__rcpt_operations_type,
        purch__spmt_operations_type,
        sales_spmt_operations_type,
        assembly_order_operations_type,
        trans__rcpt_operations_type,
        trsns__spmt_operations_type,
        
        -- Accounting
        gen__bus__posting_group,
        gen__prod__posting_group,
        currency_code,
        grade,
        sku_no_,
        cod_account,
        gift_account,
        other_charge_account,
        shipping_charge_account,
        
        -- Journal Settings
        whse__jnl_template,
        whse__jnl_batch,
        item_rec_template_name,
        item_rec_batch_name,
        req__template_name,
        req__batch_name,
        
        -- Lead Time
        lead_time__days_,
        review_time__days_,
        last_wh_entry_no_,
        
        -- System Fields
        timestamp AS erp_timestamp

    FROM source
)

SELECT * FROM renamed


