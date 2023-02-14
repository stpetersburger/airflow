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
        address3 city
  FROM  aws_s3.sales_orders so
 WHERE  NOT is_test