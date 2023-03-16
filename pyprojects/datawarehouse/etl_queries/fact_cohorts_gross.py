WITH cohort_orders AS (
    SELECT  DATE(FORMAT_DATE("%Y-%m-01", o.first_order_date_nk))  cohort_base_date_nk,
            DATE(FORMAT_DATE("%Y-%m-01", o.order_date_nk))        order_date_nk,
            COUNT(DISTINCT o.customer_reference)                  number_of_cohort_customers,
            COUNT(DISTINCT id_sales_order)                        number_of_cohort_orders,
            SUM(oi.item_aggregation_price) / 100                  cohort_gross_revenue,
      FROM  {1}.fact_orders o
      LEFT  JOIN {1}.fact_items oi
            ON o.id_sales_order = oi.fk_sales_order
     WHERE  oi.if_gross = 1
            AND NOT o.if_test_order
            AND DATE(FORMAT_DATE("%Y-%m-01", o.order_date_nk)) >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(),INTERVAl 3 HOUR)), INTERVAL 1 MONTH)
     GROUP  BY 1, 2
),
orders AS (
    SELECT  DATE(FORMAT_DATE("%Y-%m-01", o.order_date_nk))  order_date_nk,
            COUNT(DISTINCT o.customer_reference)            number_of_customers,
            COUNT(DISTINCT id_sales_order)                  number_of_orders,
            SUM(oi.item_aggregation_price) / 100            gross_revenue,
      FROM  {1}.fact_orders o
      LEFT  JOIN {1}.fact_items oi
            ON o.id_sales_order = oi.fk_sales_order
     WHERE  oi.if_gross = 1
            AND NOT o.if_test_order
            AND DATE(FORMAT_DATE("%Y-%m-01", o.order_date_nk)) >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(),INTERVAl 3 HOUR)), INTERVAL 1 MONTH)
     GROUP  BY 1
)
SELECT  co.cohort_base_date_nk,
        o.order_date_nk,
        o.number_of_customers,
        CASE WHEN o.order_date_nk=co.cohort_base_date_nk
             THEN o.number_of_customers
             ELSE co.number_of_cohort_customers
        END                                                                     number_of_cohort_customers,
        CASE WHEN o.order_date_nk=co.cohort_base_date_nk
             THEN 1
             ELSE ROUND(co.number_of_cohort_customers/o.number_of_customers,3)
        END                                                                     cohort_customers_share,
        o.number_of_orders,
        CASE WHEN o.order_date_nk=co.cohort_base_date_nk
             THEN o.number_of_orders
             ELSE co.number_of_cohort_orders
        END                                                                     number_of_cohort_orders,
        CASE WHEN o.order_date_nk=co.cohort_base_date_nk
             THEN 1
             ELSE ROUND(co.number_of_cohort_orders/o.number_of_orders,3)
        END                                                                     cohort_orders_share,
        o.gross_revenue,
        CASE WHEN o.order_date_nk=co.cohort_base_date_nk
             THEN o.gross_revenue
             ELSE co.cohort_gross_revenue
        END                                                                     cohort_gross_revenue,
        CASE WHEN o.order_date_nk=co.cohort_base_date_nk
             THEN 1
             ELSE ROUND(co.cohort_gross_revenue/o.gross_revenue,3)
        END                                                                     cohort_gross_revenue_share
  FROM  orders o
  LEFT  JOIN cohort_orders co
        USING(order_date_nk)