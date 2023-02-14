WITH items AS (
  SELECT  MIN(a.created_at)                           order_created_at,
          MIN(e.merchant_name_en)                     merchant,
          MIN(e.brand)                                brand,
          MIN(f.category_name_en)                     product_category,
          MIN(f.id_product_category)                  fk_product_category,
          MIN(a.fk_sku_simple)                        sku,
          a.id_sales_order_item                       fk_sales_order_item,
          MIN(b.order_reference)                      order_refrence,
          MIN(quantity)                               quantity,
          MAX(c.fk_sales_order_item_state)            fk_sales_order_item_state,
          MAX(d.reporting_order_item_state)           item_max_reporting_state,
          MIN(d.reporting_order_item_state)           item_min_reporting_state,
          MIN(b.currency_iso_code)                    currency,
          MIN(b.order_exchange_rate)                  exchange_rate,
          MIN(a.price)                                item_price,
          MIN(a.gross_price)                          item_gross_price,
          MIN(a.net_price)                            item_net_price,
          MIN(a.price_to_pay_aggregation)             item_aggregation_price,
          MIN(a.refundable_amount)                    item_refundable_amount,
          MIN(a.subtotal_aggregation)                 item_subtotal_aggregation,
          MIN(a.discount_amount_aggregation)          item_discount_amount_aggregation,
          MIN(a.discount_amount_full_aggregation)     item_discount_amount_full_aggregation,
          MIN(a.fk_sales_shipment)                    fk_sales_shipment,
          MIN(b.customer_reference)                   customer_reference,
          MIN(b.address3)                             city
    FROM
          aws_s3.sales_orders b
          LEFT JOIN aws_s3.sales_order_items a ON a.fk_sales_order = b.id_sales_order
          LEFT JOIN aws_s3.sales_order_item_states c ON a.id_sales_order_item = c.fk_sales_order_item
          LEFT JOIN analytics.dim_item_states d ON c.fk_sales_order_item_state = d.id_sales_order_item_state
          LEFT JOIN analytics.dim_products e ON a.fk_sku_simple = e.sku
          LEFT JOIN analytics.dim_product_categories f ON f.id_product_category = e.fk_product_category
   WHERE  NOT b.is_test
          AND DATE(b.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
          AND DATE(a.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
          AND DATE(c.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
   GROUP  BY 7
)
SELECT  DATE(order_created_at) order_date,
        merchant,
        brand,
        product_category,
        fk_product_category,
        sku,
        a.order_item_state_name_en order_item_state,
        quantity,
        order_refrence,
        CAST(fk_sales_order_item AS STRING) fk_sales_order_item,
        currency,
        exchange_rate,
        item_price,
        item_gross_price,
        item_net_price,
        item_aggregation_price,
        item_refundable_amount,
        item_subtotal_aggregation,
        item_discount_amount_aggregation,
        item_discount_amount_full_aggregation,
        customer_reference,
        COALESCE(c.city_name_en, i.city) city,
        CASE WHEN item_max_reporting_state = 0 THEN 1 ELSE 0 END if_cancelled,
        CASE WHEN item_min_reporting_state =-1 THEN 1 ELSE 0 END if_rejected,
        CASE WHEN item_max_reporting_state > 1 THEN 1 ELSE 0 END if_gross,
        CASE WHEN item_max_reporting_state > 3 THEN 1 ELSE 0 END if_sold,
        CASE WHEN item_max_reporting_state > 4 THEN 1 ELSE 0 END if_net,
        CASE WHEN item_max_reporting_state >= 2 THEN 1 ELSE 0 END if_approved,
        CASE WHEN item_max_reporting_state >= 3 THEN 1 ELSE 0 END if_ready_to_ship,
 FROM   items i
        LEFT JOIN analytics.dim_item_states a
        ON i.fk_sales_order_item_state = a.id_sales_order_item_state
        LEFT JOIN gcp_gs.map_order_cities c
        ON i.city = c.order_city_name