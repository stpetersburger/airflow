DROP TABLE IF EXISTS analytics.fact_orders;
CREATE TABLE analytics.fact_orders AS
SELECT  DATE(so.created_at) order_date_utc,
        id_sales_order,
        order_reference,
        customer_reference,
        customer_created_at                                             customer_created_at_utc,
        RANK() OVER (PARTITION BY fk_customer
                           ORDER BY so.created_at)                      customer_order_rank,
        MIN(DATE(so.created_at)) OVER (PARTITION BY fk_customer
                                       ORDER BY so.created_at)          customer_first_order_date_utc,
        LAG(DATE(so.created_at)) OVER (PARTITION BY fk_customer
                                       ORDER BY so.created_at)          customer_previous_order_date_utc,
        COALESCE(c.city_name_en, so.address3)                           city_name_en
  FROM  aws_s3.sales_orders so
  LEFT  JOIN gcp_gs.map_order_cities c
        on so.address3 = c.order_city_name
 WHERE  NOT is_test