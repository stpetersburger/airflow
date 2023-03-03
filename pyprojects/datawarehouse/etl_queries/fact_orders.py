DROP TABLE IF EXISTS analytics.fact_orders;
CREATE TABLE analytics.fact_orders AS
WITH customers_stg AS (
SELECT  is_test                                                 if_test_order,
        so.created_at                                           order_date,
        id_sales_order,
        order_reference,
        customer_reference,
        order_expense_total                                     shipping_fee,
        DATE_ADD(customer_created_at, INTERVAL 3 HOUR)          customer_created_date,
        RANK() OVER (PARTITION BY fk_customer
                         ORDER BY so.created_at)                customer_order_rank,
        MIN(so.created_at) OVER (PARTITION BY fk_customer
                                     ORDER BY so.created_at)    first_order_date,
        LAG(so.created_at) OVER (PARTITION BY fk_customer
                                     ORDER BY so.created_at)    previous_order_date,
        so.address3                                             city_name_en
  FROM  aws_s3.{0}_sales_orders so
)
SELECT  if_test_order,
        DATE_ADD(order_date, INTERVAL 3 HOUR)                   order_date,
        DATE(DATE_ADD(order_date, INTERVAL 3 HOUR))             order_date_nk,
        id_sales_order,
        order_reference,
        shipping_fee,
        COALESCE(c.city_name_en, a.city_name_en)                city_name_en,
        customer_order_rank,
        DATE_ADD(previous_order_date, INTERVAL 3 HOUR)          previous_order_date,
        DATE(DATE_ADD(previous_order_date, INTERVAL 3 HOUR))    previous_order_date_nk,
        DATE_ADD(first_order_date, INTERVAL 3 HOUR)             first_order_date,
        DATE(DATE_ADD(first_order_date, INTERVAL 3 HOUR))       first_order_date_nk,
        DATE_DIFF(order_date, previous_order_date, DAY)         previous_order_days,
        customer_reference,
        DATE_ADD(customer_created_date, INTERVAL 3 HOUR)        customer_created_date,
        DATE(DATE_ADD(customer_created_date, INTERVAL 3 HOUR))  customer_created_date_nk,
        DATE_DIFF(first_order_date, customer_created_date, DAY) customer_conversion_days
  FROM  customers_stg a
  LEFT  JOIN gcp_gs.map_order_cities c
        on a.city_name_en = c.order_city_name
        AND c.business_type = '{0}'