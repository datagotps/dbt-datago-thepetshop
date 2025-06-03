select
    a.date_created,
    a.no_,

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
    a.std_phone_no_,

    a.e_mail,
    a.blocked,


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

