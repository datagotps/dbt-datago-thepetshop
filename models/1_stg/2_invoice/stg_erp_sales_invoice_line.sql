with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_sales_invoice_line_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
        no_,
        document_no_,
        line_no_,

        case 
 when type = 2 then  'Item'
 when type = 1 then  'G/L Account'
 else 'Ask DataGo'
 end as type,


        appl__from_item_entry,
        appl__to_item_entry,
        bill_to_customer_no_,
        sell_to_customer_no_,

        customer_disc__group,

        
        

        posting_date,
        
        order_no_,
        order_line_no_,
                attached_to_line_no_,
        
        blanket_order_line_no_,
        blanket_order_no_,
        cross_reference_no_,
        cross_reference_type_no_,
                job_contract_entry_no_,
        job_no_,
        job_task_no_,
         
        shipment_line_no_,
        shipment_no_,




        amount,
        amount_including_vat,
        line_amount,
        
        line_discount__,
        line_discount_amount,
        line_discount_calculation,
        unit_cost,
        unit_cost__lcy_,
        vat__,
        vat_base_amount,


   --     type,


        

        
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        allow_invoice_disc_,
        allow_line_disc_,
        area,
        bin_code,
        
        
        
        cross_reference_type,
        
        
        customer_price_group,
        deferral_code,
        depr__until_fa_posting_date,
        depreciation_book_code,
        description,
        description_2,
        dimension_set_id,
        drop_shipment,
        duplicate_in_depreciation_book,
        exit_point,
        fa_posting_date,
        gen__bus__posting_group,
        gen__prod__posting_group,
        gross_weight,
        ic_partner_code,
        ic_partner_ref__type,
        ic_partner_reference,
        inv__discount_amount,
        item_category_code,
        location_code,
        net_weight,
        
        nonstock,
        
        
        pmt__discount_amount,
        
        posting_group,
        prepayment_line,
        price_calculation_method,
        price_description,
        product_group_code,
        purchasing_code,
        qty__per_unit_of_measure,
        quantity,
        quantity__base_,
        responsibility_center,
        return_reason_code,


        shipment_date,
        shortcut_dimension_1_code,
        shortcut_dimension_2_code,
        system_created_entry,
        tax_area_code,
        tax_category,
        tax_group_code,
        tax_liable,
        timestamp,
        transaction_specification,
        transaction_type,
        transport_method,
        unit_of_measure,
        unit_of_measure__cross_ref__,
        unit_of_measure_code,
        unit_price,
        unit_volume,
        units_per_parcel,
        use_duplication_list,
        variant_code,
        vat_bus__posting_group,
        vat_calculation_type,
        vat_clause_code,
        vat_difference,
        vat_identifier,
        vat_prod__posting_group,
        work_type_code

    from source

)

select * from renamed

--where document_no_ = 'INV00427893'