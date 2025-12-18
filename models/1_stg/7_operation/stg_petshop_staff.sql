-- =====================================================
-- STG_PETSHOP_STAFF
-- Source: ERP Staff/Employee Master Data
-- Purpose: POS staff information for transaction attribution
-- =====================================================

WITH source AS (
    SELECT * FROM {{ source(var('erp_source'), 'petshop_staff_5ecfc871_5d82_43f1_9c54_59685e82318d') }}
)

SELECT 
    -- =====================================================
    -- PRIMARY IDENTIFIERS
    -- =====================================================
    id AS staff_id,                           -- Primary key
    _systemid AS system_id,                   -- ERP system ID
    
    -- =====================================================
    -- STAFF INFORMATION
    -- =====================================================
    first_name,
    last_name,
    TRIM(CONCAT(first_name, ' ', last_name)) AS staff_full_name,
    name_on_receipt,                          -- Display name on receipts
    
    -- =====================================================
    -- STORE ASSIGNMENT
    -- =====================================================
    store_no_ AS store_code,                  -- Assigned store (DIP, FZN, WSL, etc.)
    
    -- =====================================================
    -- ROLE & PERMISSIONS
    -- =====================================================
    permission_group,                         -- MANAGER, CASHIER, TPSCHR, etc.
    CASE 
        WHEN permission_group = 'MANAGER' THEN 'Manager'
        WHEN permission_group = 'CASHIER' THEN 'Cashier'
        WHEN permission_group = 'TPSCHR' THEN 'Team Lead'
        ELSE permission_group
    END AS staff_role,
    manager_privileges,                       -- 2 = has manager access
    employment_type,                          -- Employment type code
    
    -- =====================================================
    -- STATUS FLAGS
    -- =====================================================
    CASE WHEN blocked = 0 THEN FALSE ELSE TRUE END AS is_blocked,
    CASE WHEN privacy_blocked = 0 THEN FALSE ELSE TRUE END AS is_privacy_blocked,
    date_to_be_blocked,
    
    -- =====================================================
    -- POS PERMISSIONS
    -- =====================================================
    price_override,                           -- Price override permission level
    discount_from_perm__group,                -- Discount permission
    max__discount_to_give__ AS max_discount_pct,
    max__total_discount__ AS max_total_discount,
    void_line,                                -- Can void line items
    void_transaction,                         -- Can void transactions
    return_in_transaction,                    -- Can process returns
    open_draw__without_sale,                  -- Can open drawer without sale
    
    -- =====================================================
    -- CONTACT INFORMATION
    -- =====================================================
    home_phone_no_ AS home_phone,
    work_phone_no_ AS work_phone,
    address,
    address_2,
    city,
    county,
    post_code,
    
    -- =====================================================
    -- COMPENSATION
    -- =====================================================
    hourly_rate,
    payroll_no_ AS payroll_number,
    sales_person AS salesperson_code,
    
    -- =====================================================
    -- POS SETTINGS
    -- =====================================================
    pos_interface_profile,
    pos_menu_profile,
    pos_style_profile,
    left_handed,                              -- Left-handed POS layout
    
    -- =====================================================
    -- AUDIT FIELDS
    -- =====================================================
    last_date_modified,
    
    -- =====================================================
    -- FIVETRAN METADATA
    -- =====================================================
    _fivetran_synced,
    _fivetran_deleted,
    
    -- =====================================================
    -- RECORD METADATA
    -- =====================================================
    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM source