select
    a.date_created,
    a.no_,
    a.std_phone_no_,
    -- Flag duplicate records
    CASE 
        WHEN COUNT(*) OVER (PARTITION BY a.std_phone_no_) > 1 
        THEN 'Duplicate'
        ELSE 'Unique'
    END AS duplicate_flag,
    
    -- Instance number for each phone number
    ROW_NUMBER() OVER (
        PARTITION BY a.std_phone_no_ 
        ORDER BY a.date_created ASC, a.no_ ASC
    ) AS customer_instance,
    
            -- Create a master customer ID based on phone number
        FIRST_VALUE(a.no_) OVER (
            PARTITION BY a.std_phone_no_ 
            ORDER BY 
                a.date_created ASC,  -- Prioritize earliest created
                a.no_ ASC     -- Consistent tie-breaker
        ) AS master_customer_id,




    -- Additional helpful fields for analysis
    CASE 
        WHEN COUNT(*) OVER (PARTITION BY a.std_phone_no_) > 1 
             AND ROW_NUMBER() OVER (PARTITION BY a.std_phone_no_ ORDER BY a.date_created ASC, a.no_ ASC) = 1
        THEN 'Primary'
        WHEN COUNT(*) OVER (PARTITION BY a.std_phone_no_) > 1 
        THEN 'Duplicate'
        ELSE 'Unique'
    END AS customer_record_type,
    


    a.web_customer_no_,
    b.customerid,  -- ofs

    case
        when a.web_customer_no_ = ''
        then 'offline_customer'
        when a.web_customer_no_ != '' and b.customerid is null
        then 'online_legacy_customer'
        when a.web_customer_no_ != '' and b.customerid is not null
        then 'online_ofs_customer'
        else 'undefined_segment'
    end as customer_journey_segment,

    a.name,
    a.name_2,
    a.primary_contact_no_,
    a.gen__bus__posting_group,
    a.customer_disc__group,
    a.retail_customer_group,

    a.old_web_customer_no_,
    a.webid,
    a.source_application,
    a.customer_id,
    a.created_by_user,

    a.raw_phone_no_,
    a.phone_no_status,
    

    a.e_mail,
    a.blocked,

    a.loyality_member_id,


            CASE 
        WHEN a.no_ IN (
            'C000106034', 'C000106033', 'C000123581', 'C000106036', 
            'C000124498', 'C000106035', 'C000106037', 'C000201035', 
            'C000178467', 'C000111472', 'C000106032', 'C000217299', 
            'C000168906', 'C000195622', 'C000109093', 'C000228378', 
            'C0006248', 'C000154135', 'C000123880', 'C000144552'
        ) THEN 'Anonymous' --Walk-in Customer
        ELSE 'Identified'
    END AS customer_identity_status,


from {{ ref("2_stg_erp_customer_deduped") }} as a
left join {{ ref("int_ofs_customer") }} as b on a.web_customer_no_ = b.customerid
--where std_phone_no_ != '000000000000'
--order by 5 desc
--where loyality_member_id =  'd20b11c7-efea-4ce9-a6c5-e1ca05147a06'