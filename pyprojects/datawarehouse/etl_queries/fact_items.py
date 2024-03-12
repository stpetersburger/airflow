WITH items AS (
  SELECT  MIN(a.created_at)                                                      order_created_at,
          MIN(e.merchant_name_en)                                                merchant,
          MIN(e.brand)                                                           brand,
          MAX(f.id_product_category)                                             fk_product_category,
          MIN(a.fk_sku_simple)                                                   sku,
          a.id_sales_order_item                                                  fk_sales_order_item,
          MIN(b.order_reference)                                                 order_reference,
          MIN(b.id_sales_order)                                                  fk_sales_order,
          MIN(quantity)                                                          quantity,
          MAX(c.fk_sales_order_item_state)                                       fk_sales_order_item_state,
          MAX(c.updated_at)                                                      status_last_updated_date,
          MAX(CONCAT(d.reporting_order_item_state,'@',c.updated_at))             item_max_reporting_state,
          MIN(CONCAT(d.reporting_order_item_state,'@',c.updated_at))             item_min_reporting_state,
          MIN(b.currency_iso_code)                                               currency,
          {exchange_rate}                                                        exchange_rate,
          MIN(b.cart_note)                                                       coupon_code,
          MIN(a.price)                                                           item_price,
          MIN(a.gross_price)                                                     item_gross_price,
          MIN(a.net_price)                                                       item_net_price,
          MIN(a.price_to_pay_aggregation)                                        item_aggregation_price,
          MIN(a.refundable_amount)                                               item_refundable_amount,
          MIN(a.subtotal_aggregation)                                            item_subtotal_aggregation,
          MIN(a.discount_amount_aggregation)                                     item_discount_amount_aggregation,
          MIN(a.discount_amount_full_aggregation)                                item_discount_amount_full_aggregation,
          MIN(a.fk_sales_shipment)                                               fk_sales_shipment,
          MIN(b.customer_reference)                                              customer_reference,
          TRIM(MIN(CASE WHEN '{0}'='b2c' THEN b.address3 ELSE b.address2 END))   city_name,
          {points_redeemed}                                                      points_redeemed,
          {channel}                                                              channel,
          MIN(b.fk_country)                                                      address_country_nk,
          'spryker'                                                              engine_name_en
    FROM
          aws_s3.{0}_sales_orders b
          LEFT JOIN aws_s3.{0}_sales_order_items a ON a.fk_sales_order = b.id_sales_order
          LEFT JOIN aws_s3.{0}_sales_order_item_states c ON a.id_sales_order_item = c.fk_sales_order_item
          LEFT JOIN {1}.dim_item_states d ON c.fk_sales_order_item_state = d.id_sales_order_item_state
          LEFT JOIN {1}.dim_products e ON a.fk_sku_simple = e.sku
          LEFT JOIN {1}.dim_product_categories f ON f.id_product_category = e.fk_product_category
   WHERE  DATE(b.created_at) >= DATE_SUB(DATE(CURRENT_TIMESTAMP()), INTERVAL {incr_interval})
          AND DATE(a.created_at) >= DATE_SUB(DATE(CURRENT_TIMESTAMP()), INTERVAL {incr_interval})
          AND DATE(c.created_at) >= DATE_SUB(DATE(CURRENT_TIMESTAMP()), INTERVAL {incr_interval})
          AND NOT b.is_test
          AND d.reporting_order_item_state < 6
   GROUP  BY 6

UNION ALL

SELECT  MIN(b.created_at)                                                      order_created_at,
        MIN(e.merchant_name_en)                                                merchant,
        MIN(e.brand)                                                           brand,
        MAX(f.id_product_category)                                             fk_product_category,
        MIN(e.sku)                                                             sku,
        CAST(MIN(a.fk_sales_order) AS INT64) * 1000000000 + a.fk_sku_simple    fk_sales_order_item,
        COALESCE(MIN(b_parent.order_reference),
                 CAST(MIN(b.fk_parent_order) AS STRING))                       order_reference, #if parent order was missing in streaming
        a.fk_sales_order                                                       fk_sales_order,
        MIN(a.remained_quantity)                                               quantity,
        MAX(d.id_sales_order_item_state)                                       fk_sales_order_item_state,
        MAX(a.updated_at)                                                      status_last_updated_date,
        MAX(CONCAT(d.reporting_order_item_state,'@',a.updated_at))             item_max_reporting_state,
        MIN(CONCAT(d.reporting_order_item_state,'@',a.updated_at))             item_min_reporting_state,
        MIN(b.currency_iso_code)                                               currency,
        MIN(COALESCE(CAST(b.order_exchange_rate AS DECIMAL),0))                exchange_rate,
        MIN(c.discount_type)                                                   coupon_code,
        MIN(c.gross_price)                                                     item_price,
        MIN(c.gross_price)                                                     item_gross_price,
        MIN(c.net_price)                                                       item_net_price,
        MIN(c.net_price)                                                       item_aggregation_price,
        MIN(c.net_price)                                                       item_refundable_amount,
        MIN(c.net_price)                                                       item_subtotal_aggregation,
        MIN(c.discount_amount)                                                 item_discount_amount_aggregation,
        MIN(c.discount_amount)                                                 item_discount_amount_full_aggregation,
        -9999                                                                  fk_sales_shipment,
        MIN(b.customer_reference)                                              customer_reference,
        TRIM(MIN(b.city_name))                                                 city_name,
        MIN(b.points_redeemed)                                                 points_redeemed,
        MIN(b.channel_name)                                                    channel,
        111                                                                    address_country_nk,
        'vendure'                                                              engine_name_en
  FROM  aws_s3.{0}_sales_order_item_states_vendure a
  LEFT  JOIN aws_s3.{0}_sales_orders_vendure b
        ON a.fk_sales_order = b.id_sales_order
        JOIN aws_s3.b2c_sales_orders_vendure b_parent
        ON b.fk_parent_order = b_parent.id_sales_order
  LEFT  JOIN aws_s3.{0}_sales_order_items_vendure c
        ON a.fk_sales_order = c.fk_sales_order
        AND a.fk_sku_simple = c.fk_sku_simple
  LEFT  JOIN {1}.dim_item_states d
        ON a.fk_sales_order_item_state = CAST(d.id_sales_order_item_state AS STRING)
  LEFT  JOIN {1}.dim_products e ON c.sk_sku_simple = e.sku
  LEFT  JOIN {1}.dim_product_categories f
        ON f.id_product_category = e.fk_product_category
 WHERE  b.fk_parent_order IS NOT NULL # only non-parent (child) orders
        AND NOT b.is_test
        AND d.reporting_order_item_state < 6
        AND DATE(a.created_at) >= DATE_SUB(DATE(CURRENT_TIMESTAMP()), INTERVAL {incr_interval})
        AND DATE(b.created_at) >= DATE_SUB(DATE(CURRENT_TIMESTAMP()), INTERVAL {incr_interval})
        AND DATE(b_parent.created_at) >= DATE_SUB(DATE(CURRENT_TIMESTAMP()), INTERVAL {incr_interval})
        AND DATE(c.created_at) >= DATE_SUB(DATE(CURRENT_TIMESTAMP()), INTERVAL {incr_interval})
        AND engine_name_en = 'vendure'
 GROUP  BY a.fk_sales_order, a.fk_sku_simple
),

items_stg AS (
SELECT  DATE_ADD(order_created_at, INTERVAL 3 HOUR)                                 order_date,
        DATE(DATE_ADD(order_created_at, INTERVAL 3 HOUR))                           order_date_nk,
        merchant,
        brand,
        fk_product_category,
        sku,
        CASE WHEN CURRENT_DATE() > DATE(status_last_updated_date) + 14
                  AND CAST(SPLIT(item_max_reporting_state,'@')[OFFSET(0)] AS INT64) = 5
                  AND CAST(SPLIT(item_min_reporting_state,'@')[OFFSET(0)] AS INT64) > 0
             THEN 'closed'
             ELSE a.order_item_state_name_en
        END                                                                         order_item_state,
        CASE WHEN CURRENT_DATE() > DATE(status_last_updated_date) + 14
                  AND CAST(SPLIT(item_max_reporting_state,'@')[OFFSET(0)] AS INT64) = 5
                  AND CAST(SPLIT(item_min_reporting_state,'@')[OFFSET(0)] AS INT64) > 0
             THEN CASE WHEN '{0}'='b2c'
                       THEN CASE WHEN i.engine_name_en='spryker'
                                 THEN 19
                                ELSE 31
                            END
                       ELSE 11
                  END
             ELSE i.fk_sales_order_item_state
        END                                                                         fk_sales_order_item_state,
        DATE(DATE_ADD(status_last_updated_date, INTERVAL 3 HOUR))                   status_last_changed_date_nk,
        quantity,
        order_reference,
        fk_sales_order,
        CAST(fk_sales_order_item AS STRING)                                         id_sales_order_item,
        CASE WHEN CAST(SPLIT(item_min_reporting_state,'@')[OFFSET(0)] AS INT64) > 0 THEN
             CASE WHEN CAST(SPLIT(item_max_reporting_state,'@')[OFFSET(0)] AS INT64) < 5 THEN
                  DATE_DIFF(CURRENT_TIMESTAMP(), order_created_at, DAY)
             ELSE DATE_DIFF(status_last_updated_date, order_created_at, DAY)
             END
        ELSE DATE_DIFF(status_last_updated_date, order_created_at, DAY)
        END                                                                         days_in_process_num,
        CASE WHEN CAST(SPLIT(item_max_reporting_state,'@')[OFFSET(0)] AS INT64) = 5
             THEN DATE_DIFF(CAST(SPLIT(item_max_reporting_state,'@')[OFFSET(1)] AS TIMESTAMP), order_created_at, DAY)
        ELSE NULL
        END                                                                         days_delivery_num,
        currency,
        exchange_rate,
        item_price,
        coupon_code,
        item_gross_price,
        item_net_price,
        item_aggregation_price,
        item_refundable_amount,
        item_subtotal_aggregation,
        item_discount_amount_aggregation,
        item_discount_amount_full_aggregation,
        i.customer_reference                                                                           customer_reference,
        COALESCE(c.city_name_en, i.city_name)                                                          city_name_en,
        CASE WHEN CAST(SPLIT(item_min_reporting_state,'@')[OFFSET(0)] AS INT64) = 0 THEN 1 ELSE 0 END  if_cancelled,
        CASE WHEN CAST(SPLIT(item_min_reporting_state,'@')[OFFSET(0)] AS INT64) =-1 THEN 1 ELSE 0 END  if_rejected,
        CASE WHEN CAST(SPLIT(item_max_reporting_state,'@')[OFFSET(0)] AS INT64) > 1 THEN 1 ELSE 0 END  if_gross,
        CASE WHEN CAST(SPLIT(item_max_reporting_state,'@')[OFFSET(0)] AS INT64) > 3 THEN 1 ELSE 0 END  if_sold,
        CASE WHEN CAST(SPLIT(item_max_reporting_state,'@')[OFFSET(0)] AS INT64) > 4 THEN 1 ELSE 0 END  if_net,
        CASE WHEN CAST(SPLIT(item_max_reporting_state,'@')[OFFSET(0)] AS INT64) >= 2 THEN 1 ELSE 0 END if_approved,
        CASE WHEN CAST(SPLIT(item_max_reporting_state,'@')[OFFSET(0)] AS INT64) >= 3 THEN 1 ELSE 0 END if_ready_to_ship,
        CASE WHEN COALESCE(points_redeemed,0) > 0 THEN 1 ELSE 0 END                                    if_points_used_in_order,
        channel,
        COALESCE(cust.customer_category,'Other')                                                       customer_category,
        COALESCE(points_redeemed,0)                                                                    order_points_redeemed,
        SUM(CASE WHEN CAST(SPLIT(item_min_reporting_state,'@')[OFFSET(0)] AS INT64) > 0
                 THEN item_aggregation_price
                 ELSE 0
            END ) OVER (PARTITION BY fk_sales_order)                                                  order_non_cancelled_value,
        address_country_nk
 FROM   items i
        LEFT JOIN {1}.dim_item_states a
        ON i.fk_sales_order_item_state = a.id_sales_order_item_state
        LEFT JOIN gcp_gs.map_order_cities c
        ON i.city_name = c.order_city_name
        AND c.business_type = '{0}'
        LEFT JOIN gcp_gs.map_customers cust
        ON i.customer_reference = cust.customer_reference
        AND cust.business_type = '{0}'
        AND i.engine_name_en = 'vendure'
)

SELECT  order_date,
        order_date_nk,
        merchant,
        brand,
        fk_product_category,
        sku,
        order_item_state,
        fk_sales_order_item_state,
        status_last_changed_date_nk,
        quantity,
        order_reference,
        fk_sales_order,
        id_sales_order_item,
        days_in_process_num,
        days_delivery_num,
        currency,
        exchange_rate,
        item_price,
        coupon_code,
        item_gross_price,
        item_net_price,
        item_aggregation_price,
        item_refundable_amount,
        item_subtotal_aggregation,
        item_discount_amount_aggregation,
        item_discount_amount_full_aggregation,
        customer_reference,
        city_name_en,
        if_cancelled,
        if_rejected,
        if_gross,
        if_sold,
        if_net,
        if_approved,
        if_ready_to_ship,
        if_points_used_in_order,
        channel,
        customer_category,
        CASE WHEN if_points_used_in_order = 1 AND order_non_cancelled_value > 0
                                              AND if_cancelled + if_rejected = 0
             THEN CASE WHEN order_points_redeemed>order_non_cancelled_value
                       THEN ROUND(item_aggregation_price/order_non_cancelled_value,4)
                       ELSE ROUND(order_points_redeemed*(item_aggregation_price/order_non_cancelled_value),4)
                  END
             ELSE 0
        END  order_points_redeemed,
        address_country_nk
  FROM  items_stg
 ORDER  BY 1