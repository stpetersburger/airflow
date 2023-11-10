SELECT  DATE(DATE_ADD(CAST(instance_date as TIMESTAMP), INTERVAL 4 HOUR))  transaction_date_nk,
        transaction_number                                                 transaction_nk,
        group_en                                                           transaction_type_name_en,
        procedure_en                                                       transaction_subtype_name_en,
        usage_en                                                           deal_type_name_en,
        area_en                                                            district_name_en,
        prop_type_en                                                       property_type_name_en,
        prop_sb_type_en                                                    property_subtype_name_en,
        rooms_en                                                           property_room_number_en,
        is_offplan_en                                                      property_completion_name_en,
        CAST(trans_value AS DECIMAL)                                       transaction_value_aed,
        project_en                                                         building_name_en,
  FROM  scrapers.dld_transactions
 WHERE  DATE(DATE_ADD(CAST(instance_date as TIMESTAMP), INTERVAL 4 HOUR)) >=
        DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR)), INTERVAL {incr_interval})