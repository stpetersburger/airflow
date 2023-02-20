DROP TABLE IF EXISTS analytics.fact_orders;
CREATE TABLE analytics.fact_orders AS
WITH customers_stg AS (
SELECT  so.created_at                                           order_date,
        id_sales_order,
        order_reference,
        customer_reference,
        customer_created_at                                     customer_created_date,
        RANK() OVER (PARTITION BY fk_customer
                         ORDER BY so.created_at)                customer_order_rank,
        MIN(so.created_at) OVER (PARTITION BY fk_customer
                                     ORDER BY so.created_at)    first_order_date,
        LAG(so.created_at) OVER (PARTITION BY fk_customer
                                     ORDER BY so.created_at)    previous_order_date,
        so.address3                                             city_name_en
  FROM  aws_s3.sales_orders so
 WHERE  NOT is_test
)
SELECT  order_date,
        id_sales_order,
        order_reference,
        COALESCE(c.city_name_en, a.city_name_en) city_name_en,
        customer_order_rank,
        previous_order_date,
        first_order_date,
        DATE_DIFF(order_date, previous_order_date, DAY) previous_order_days,
        customer_reference,
        customer_created_date,
        DATE_DIFF(first_order_date, customer_created_date, DAY) customer_conversion_days
  FROM  customers_stg a
  LEFT  JOIN gcp_gs.map_order_cities c
        on a.city_name_en = c.order_city_name