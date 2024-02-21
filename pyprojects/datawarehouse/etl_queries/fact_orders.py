WITH customers_stg AS (
SELECT  is_test                                                        if_test_order,
        so.created_at                                                  order_date,
        id_sales_order,
        order_reference,
        customer_reference,
        order_expense_total                                            shipping_fee,
        DATE_ADD(customer_created_at, INTERVAL 3 HOUR)                 customer_created_date,
        RANK() OVER (PARTITION BY fk_customer
                         ORDER BY so.created_at)                       customer_order_rank,
        MIN(so.created_at) OVER (PARTITION BY fk_customer
                                     ORDER BY so.created_at)           first_order_date,
        LAG(so.created_at) OVER (PARTITION BY fk_customer
                                     ORDER BY so.created_at)           previous_order_date,
        CASE WHEN '{0}' = 'b2c' THEN so.address3 ELSE so.address2 END  city_name_en
  FROM  aws_s3.{0}_sales_orders so

UNION ALL

SELECT  is_test                                                        if_test_order,
        so.created_at                                                  order_date,
        id_sales_order,
        order_reference,
        customer_reference,
        0                                                              shipping_fee,
        DATE_ADD(customer_created_at, INTERVAL 3 HOUR)                 customer_created_date,
        RANK() OVER (PARTITION BY fk_customer
                         ORDER BY so.created_at)                       customer_order_rank,
        MIN(so.created_at) OVER (PARTITION BY fk_customer
                                     ORDER BY so.created_at)           first_order_date,
        LAG(so.created_at) OVER (PARTITION BY fk_customer
                                     ORDER BY so.created_at)           previous_order_date,
        city_name                                                      city_name_en
  FROM  aws_s3.{0}_sales_orders_vendure so
 WHERE  created_at>='2024-01-01'

)
SELECT  MAX(if_test_order)                                           if_test_order,
        MAX(DATE_ADD(order_date, INTERVAL 3 HOUR))                   order_date,
        MAX(DATE(DATE_ADD(order_date, INTERVAL 3 HOUR)))             order_date_nk,
        id_sales_order,
        MAX(order_reference)                                         order_reference,
        MAX(shipping_fee)                                            shipping_fee,
        MAX(COALESCE(c.city_name_en, a.city_name_en))                city_name_en,
        MAX(customer_order_rank)                                     customer_order_rank,
        MAX(DATE_ADD(previous_order_date, INTERVAL 3 HOUR))          previous_order_date,
        MAX(DATE(DATE_ADD(previous_order_date, INTERVAL 3 HOUR)))    previous_order_date_nk,
        MAX(DATE_ADD(first_order_date, INTERVAL 3 HOUR))             first_order_date,
        MAX(DATE(DATE_ADD(first_order_date, INTERVAL 3 HOUR)))       first_order_date_nk,
        MAX(DATE_DIFF(order_date, previous_order_date, DAY))         previous_order_days,
        MAX(customer_reference)                                      customer_reference,
        MAX(DATE_ADD(customer_created_date, INTERVAL 3 HOUR))        customer_created_date,
        MAX(DATE(DATE_ADD(customer_created_date, INTERVAL 3 HOUR)))  customer_created_date_nk,
        MAX(DATE_DIFF(first_order_date, customer_created_date, DAY)) customer_conversion_days
  FROM  customers_stg a
  LEFT  JOIN gcp_gs.map_order_cities c
        on a.city_name_en = c.order_city_name
        AND c.business_type = '{0}'
 GROUP  BY 4