-- =====================================================
-- dim_store: Store Dimension
-- Source: ERP Location table (petshop_location)
-- Contains store information, operating hours, services, and classification
-- =====================================================

WITH 

source AS (
    SELECT * FROM {{ ref('stg_erp_location') }}
),

-- Enrich with business classifications
stores AS (
    SELECT
        -- Primary Key
        location_code AS store_no_,
        
        -- Store Name & Description
        CASE 
            WHEN location_code = 'DIP' THEN 'The Pet Shop - Dubai Investment Park (Mega Store)'
            WHEN location_code = 'FZN' THEN 'The Pet Shop - Al Forsan'
            WHEN location_code = 'MRI' THEN 'The Pet Shop - Marina Walk'
            WHEN location_code = 'UMSQ' THEN 'The Pet Shop - Umm Suqeim'
            WHEN location_code = 'WSL' THEN 'The Pet Shop - Al Wasl'
            WHEN location_code = 'CRK' THEN 'The Pet Shop - Corniche Road'
            WHEN location_code = 'CREEK' THEN 'The Pet Shop - Creek'
            WHEN location_code = 'REM' THEN 'The Pet Shop - Remraam'
            WHEN location_code = 'SZR' THEN 'The Pet Shop - Sheikh Zayed Road'
            WHEN location_code = 'RAK' THEN 'The Pet Shop - Ras Al Khaimah'
            WHEN location_code = 'DSO' THEN 'The Pet Shop - Dubai Silicon Oasis'
            WHEN location_code = 'JVC' THEN 'The Pet Shop - Jumeirah Village Circle'
            WHEN location_code = 'DLM' THEN 'The Pet Shop - Dalma Mall'
            WHEN location_code = 'MAJ' THEN 'The Pet Shop - Matajer Al Juraina'
            WHEN location_code = 'GRM' THEN 'The Pet Shop - In-Store Grooming'
            WHEN location_code = 'MOBILE' THEN 'The Pet Shop - Mobile Grooming'
            WHEN location_code = 'S GROOMING' THEN 'The Pet Shop - Store Grooming Services'
            WHEN location_code = 'HQW' THEN 'Headquarters Warehouse'
            WHEN location_code = 'HQW2' THEN 'Headquarters Warehouse 2'
            WHEN location_code = 'DAMAGE' THEN 'Damaged Goods Warehouse'
            WHEN location_code = 'QUAR' THEN 'Quarantine Storage'
            WHEN location_code = 'PRL' THEN 'Pet Relocation Services'
            WHEN location_code = 'ASFX' THEN 'The Pet Shop - ASFX'
            WHEN location_code = 'CLM' THEN 'The Pet Shop - CLM'
            ELSE location_code
        END AS store_name,
        
        -- Store Type from ERP (standardized)
        CASE 
            WHEN UPPER(COALESCE(store_type, '')) = 'STORE' THEN 'Retail Store'
            WHEN location_code IN ('DIP', 'FZN', 'MRI', 'UMSQ', 'WSL', 'CRK', 'CREEK', 'REM', 'SZR', 'RAK', 'DSO', 'JVC', 'DLM', 'MAJ', 'ASFX') THEN 'Retail Store'
            WHEN location_code IN ('GRM', 'S GROOMING') THEN 'Grooming Center'
            WHEN location_code = 'MOBILE' THEN 'Mobile Service'
            WHEN location_code IN ('HQW', 'HQW2', 'DAMAGE', 'QUAR', 'IMP-SHORT', 'HQWCD', 'IN-TRANSIT') THEN 'Warehouse'
            WHEN location_code = 'PRL' THEN 'Pet Relocation'
            WHEN location_code = 'MRK' THEN 'Marketing'
            WHEN location_code = 'PRO-MAIN' THEN 'Projects & Maintenance'
            WHEN location_code = 'AMAZON FBA' OR location_code = 'AFBA' THEN 'Amazon FBA'
            ELSE 'Other'
        END AS store_type_category,
        
        -- Original store_type from ERP
        store_type AS erp_store_type,
        
        -- Store Type Sort Order
        CASE 
            WHEN UPPER(COALESCE(store_type, '')) = 'STORE' OR location_code IN ('DIP', 'FZN', 'MRI', 'UMSQ', 'WSL', 'CRK', 'CREEK', 'REM', 'SZR', 'RAK', 'DSO', 'JVC', 'DLM', 'MAJ', 'ASFX') THEN 1
            WHEN location_code IN ('GRM', 'S GROOMING') THEN 2
            WHEN location_code = 'MOBILE' THEN 3
            WHEN location_code IN ('HQW', 'HQW2', 'DAMAGE', 'QUAR', 'IMP-SHORT') THEN 4
            ELSE 5
        END AS store_type_sort,
        
        -- Operating Hours (from ERP)
        store_hour AS operating_hours,
        
        -- Store Services (from ERP)
        store_services,
        
        -- Parse individual services from store_services
        CASE WHEN store_services LIKE '%Pet Grooming%' THEN TRUE ELSE FALSE END AS has_grooming_service,
        CASE WHEN store_services LIKE '%Café%' OR store_services LIKE '%Cafe%' THEN TRUE ELSE FALSE END AS has_cafe,
        CASE WHEN store_services LIKE '%Paw Park%' THEN TRUE ELSE FALSE END AS has_paw_park,
        CASE WHEN store_services LIKE '%Aquatics%' THEN TRUE ELSE FALSE END AS has_aquatics,
        CASE WHEN store_services LIKE '%Marine%' THEN TRUE ELSE FALSE END AS has_aquatics_marine,
        CASE WHEN store_services LIKE '%Fresh Water%' THEN TRUE ELSE FALSE END AS has_aquatics_freshwater,
        CASE WHEN store_services LIKE '%Pet Name Tags%' THEN TRUE ELSE FALSE END AS has_pet_name_tags,
        CASE WHEN store_services LIKE '%Click%Collect%' OR store_services LIKE '%C&C%' THEN TRUE ELSE FALSE END AS has_click_and_collect,
        CASE WHEN store_services LIKE '%Pet Relocation%' THEN TRUE ELSE FALSE END AS has_pet_relocation,
        CASE WHEN store_services LIKE '%Pet Adoption%' THEN TRUE ELSE FALSE END AS has_pet_adoption,
        
        -- Emirate (from ERP state field - standardized)
        CASE 
            WHEN UPPER(COALESCE(emirate, '')) LIKE '%DUBAI%' THEN 'Dubai'
            WHEN UPPER(COALESCE(emirate, '')) LIKE '%ABU%DHABI%' THEN 'Abu Dhabi'
            WHEN UPPER(COALESCE(emirate, '')) LIKE '%RAS%' THEN 'Ras Al Khaimah'
            WHEN location_code IN ('DIP', 'MRI', 'UMSQ', 'WSL', 'REM', 'SZR', 'DSO', 'JVC', 'GRM', 'MOBILE', 'HQW', 'HQW2', 'CREEK', 'ASFX') THEN 'Dubai'
            WHEN location_code IN ('FZN', 'DLM', 'CRK', 'MAJ') THEN 'Abu Dhabi'
            WHEN location_code = 'RAK' THEN 'Ras Al Khaimah'
            ELSE 'Unknown'
        END AS emirate,
        
        -- Original state from ERP
        emirate AS erp_emirate,
        
        -- Region Classification
        CASE 
            WHEN UPPER(COALESCE(emirate, '')) LIKE '%DUBAI%' OR location_code IN ('DIP', 'MRI', 'UMSQ', 'WSL', 'REM', 'SZR', 'DSO', 'JVC', 'GRM', 'MOBILE', 'HQW', 'HQW2', 'CREEK') THEN 'Dubai'
            WHEN UPPER(COALESCE(emirate, '')) LIKE '%ABU%' OR location_code IN ('FZN', 'DLM', 'CRK', 'MAJ') THEN 'Abu Dhabi'
            WHEN UPPER(COALESCE(emirate, '')) LIKE '%RAS%' OR location_code = 'RAK' THEN 'Northern Emirates'
            ELSE 'Other'
        END AS region,
        
        -- Region Sort Order
        CASE 
            WHEN UPPER(COALESCE(emirate, '')) LIKE '%DUBAI%' OR location_code IN ('DIP', 'MRI', 'UMSQ', 'WSL', 'REM', 'SZR', 'DSO', 'JVC', 'GRM', 'MOBILE', 'HQW', 'HQW2', 'CREEK') THEN 1
            WHEN UPPER(COALESCE(emirate, '')) LIKE '%ABU%' OR location_code IN ('FZN', 'DLM', 'CRK', 'MAJ') THEN 2
            WHEN UPPER(COALESCE(emirate, '')) LIKE '%RAS%' OR location_code = 'RAK' THEN 3
            ELSE 4
        END AS region_sort,
        
        -- Store Dates
        store_start_date,
        store_end_date,
        
        -- Store Image
        store_image_url,
        
        -- Store Priority
        priority AS store_priority,
        
        -- Active & Capability Flags
        CASE WHEN COALESCE(is_disabled, 0) = 0 THEN TRUE ELSE FALSE END AS is_active,
        CASE WHEN COALESCE(is_pickup_enabled, 0) = 1 THEN TRUE ELSE FALSE END AS is_pickup_enabled,
        CASE WHEN COALESCE(is_web_store, 0) = 1 THEN TRUE ELSE FALSE END AS is_web_store,
        CASE WHEN COALESCE(is_web_location, 0) = 1 THEN TRUE ELSE FALSE END AS is_web_location,
        
        -- Shopify Integration
        shopify_location_id,
        shopify_b2b_location_id,
        
        -- Sync Status
        store_synced_at,
        location_synced_at,
        CASE WHEN COALESCE(store_sync_with_web, 0) = 1 THEN TRUE ELSE FALSE END AS store_sync_with_web,
        CASE WHEN COALESCE(location_sync_with_web, 0) = 1 THEN TRUE ELSE FALSE END AS location_sync_with_web,
        
        -- Classification Flags
        CASE 
            WHEN UPPER(COALESCE(store_type, '')) = 'STORE' OR location_code IN ('DIP', 'FZN', 'MRI', 'UMSQ', 'WSL', 'CRK', 'CREEK', 'REM', 'SZR', 'RAK', 'DSO', 'JVC', 'DLM', 'MAJ', 'ASFX') THEN TRUE
            ELSE FALSE
        END AS is_retail_store,
        
        CASE 
            WHEN location_code IN ('GRM', 'S GROOMING', 'MOBILE') THEN TRUE
            ELSE FALSE
        END AS is_grooming_service,
        
        CASE 
            WHEN location_code IN ('HQW', 'HQW2', 'DAMAGE', 'QUAR', 'IMP-SHORT', 'HQWCD', 'IN-TRANSIT') THEN TRUE
            ELSE FALSE
        END AS is_warehouse,
        
        -- Warehouse & Operations (for internal use)
        warehouse_code,
        site_id,
        default_cost_center,
        replenishment_priority,
        putaway_priority,
        
        -- Report Metadata
        DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at

    FROM source
    WHERE location_code IS NOT NULL
)

SELECT * FROM stores

