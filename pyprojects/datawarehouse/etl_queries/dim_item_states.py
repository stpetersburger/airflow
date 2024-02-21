SELECT  a.fk_sales_order_item_state                              id_sales_order_item_state,
        MAX(b.engine_order_state_name_en)                        order_item_state_name_en,
        MAX(CAST(b.reporting_order_item_state AS INT64))         reporting_order_item_state,
        MAX(b.reporting_order_item_state_name_en)                reporting_order_item_state_name_en,
        'spryker'                                                engine_name_en
  FROM  aws_s3.{0}_sales_order_item_states a
  LEFT  JOIN gcp_gs.map_order_item_status b
        ON a.fk_sales_order_item_state = CAST(b.fk_sales_order_item_state AS INT64)
 WHERE  DATE(a.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
        AND b.fk_sales_order_item_state is not null
        AND b.business_type = '{0}'
        AND LOWER(b.engine_name_en) = 'spryker'
 GROUP  BY 1

UNION ALL

SELECT  CAST(a.fk_sales_order_item_state AS INT64)               id_sales_order_item_state,
        MAX(b.engine_order_state_name_en)                        order_item_state_name_en,
        MAX(CAST(b.reporting_order_item_state AS INT64))         reporting_order_item_state,
        MAX(b.reporting_order_item_state_name_en)                reporting_order_item_state_name_en,
        'vendure'                                                engine_name_en
  FROM  aws_s3.{0}_sales_order_item_states_vendure a
  LEFT  JOIN gcp_gs.map_order_item_status b
        ON a.fk_sales_order_item_state = CAST(b.fk_sales_order_item_state AS STRING)
 WHERE  DATE(a.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
        AND b.fk_sales_order_item_state is not null
        AND b.business_type = '{0}'
        AND LOWER(b.engine_name_en) = 'vendure'
 GROUP  BY 1
