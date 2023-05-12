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
          {channel}                                                              channel
    FROM
          aws_s3.{0}_sales_orders b
          LEFT JOIN aws_s3.{0}_sales_order_items a ON a.fk_sales_order = b.id_sales_order
          LEFT JOIN aws_s3.{0}_sales_order_item_states c ON a.id_sales_order_item = c.fk_sales_order_item
          LEFT JOIN {1}.dim_item_states d ON c.fk_sales_order_item_state = d.id_sales_order_item_state
          LEFT JOIN {1}.dim_products e ON a.fk_sku_simple = e.sku
          LEFT JOIN {1}.dim_product_categories f ON f.id_product_category = e.fk_product_category
   WHERE  DATE(DATE_ADD(b.created_at, INTERVAL 3 HOUR)) >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)), INTERVAL {incr_interval})
          AND DATE(DATE_ADD(a.created_at, INTERVAL 3 HOUR)) >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)), INTERVAL {incr_interval})
          AND DATE(DATE_ADD(c.created_at, INTERVAL 3 HOUR)) >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)), INTERVAL {incr_interval})
          AND NOT b.is_test
          AND d.reporting_order_item_state < 6
   GROUP  BY 6
)
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
             THEN CASE WHEN '{0}'='b2c' THEN 19 ELSE 11 END
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
        points_redeemed                                                                                order_points_redeemed,
        channel,
        COALESCE(cust.customer_category,'Other')                                                       customer_category
 FROM   items i
        LEFT JOIN {1}.dim_item_states a
        ON i.fk_sales_order_item_state = a.id_sales_order_item_state
        LEFT JOIN gcp_gs.map_order_cities c
        ON i.city_name = c.order_city_name
        AND c.business_type = '{0}'
        LEFT JOIN gcp_gs.map_customers cust
        ON i.customer_reference = cust.customer_reference
        AND cust.business_type = '{0}'