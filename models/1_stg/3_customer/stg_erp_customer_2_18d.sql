with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_customer_5ecfc871_5d82_43f1_9c54_59685e82318d') }}

),

renamed as (

    select
        no_,
        created_by_user,
        date_created,

        customer_id,

        retail_customer_group,  --GROUPA GROUPB GROUPC GROUPD GROUPE GROUPF GROUPG GROUPH GROUPI
        mobile_phone_no_, --all null
        


        
        amtchargedonposint,
        amtchargedpostedint,
        balancelcyint,
        
        
        
        daytime_phone_no_,
        default_weight,
        house_apartment_no_,
        
        other_tender_in_finalizing,
        post_as_shipment,
        print_document_invoice,
        reason_code,
        restriction_functionality,
        
        timestamp,
        transaction_limit
        _fivetran_deleted,
        _fivetran_synced,

    from source

)

select * from renamed



--where no_ = 'C000066928'

--where customer_id != ''

--where daytime_phone_no_   != ''

--where no_ = 'BCN/2025/000070'

