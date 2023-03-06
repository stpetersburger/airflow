#sales_order_item_state
SELECT  sooish.fk_oms_order_item_state id_sales_order_item_state,
        fk_sales_order,
        fk_sales_order_item,
        soi.created_at,
        sooish.created_at updated_at
  FROM  spy_oms_order_item_state_history sooish
  LEFT  JOIN spy_sales_order_item soi
ON soi.id_sales_order_item = sooish.fk_sales_order_item
--WHERE date(sooish.created_at) = '2023-02-22';

#sales_order_item
SELECT
sku fk_sku_simple,
merchant_reference merchant_id,
fk_sales_order,
id_sales_order_item,
fk_sales_order_item_bundle,
fk_sales_shipment,
quantity,
is_quantity_splittable,
canceled_amount,
discount_amount_aggregation,
discount_amount_full_aggregation,
gross_price,
net_price,
price,
price_to_pay_aggregation,
product_offer_reference,
refundable_amount,
product_option_price_aggregation,
subtotal_aggregation,
tax_amount,
tax_amount_full_aggregation,
created_at
FROM spy_sales_order_item
--WHERE date(created_at) = '2023-02-22';

#sales_order
SELECT
fk_country,
sc.id_customer                  fk_customer,
id_sales_order,
is_test,
order_reference,
so.fk_locale                    fk_locale,
cart_note,
currency_iso_code,
order_exchange_rate,
order_custom_reference,
so.customer_reference           customer_reference,
oms_processor_identifier,
sot.id_sales_order_totals       id_sales_order_totals,
sot.discount_total              discount_total,
sot.grand_total                 grand_total,
sot.order_expense_total         order_expense_total,
sot.refund_total                refund_total,
sot.subtotal                    subtotal,
sot.tax_total                   tax_total,
se.id_sales_expense             id_sales_expense,
se.discount_amount_aggregation  discount_amount_aggregation,
se.gross_price                  gross_price,
se.name                         name,
se.net_price                    net_price,
se.price                        price,
se.price_to_pay_aggregation     price_to_pay_aggregation,
se.refundable_amount            refundable_amount,
se.tax_amount                   tax_amount,
soa.id_sales_order_address      id_sales_order_address,
soa.fk_region                   fk_region,
soa.address1                    address1,
soa.address2                    address2,
soa.address3                    address3,
sc.created_at                   customer_created_at,
so.created_at                   created_at
FROM  spy_sales_order so
LEFT  JOIN spy_customer sc
      ON so.customer_reference = sc.customer_reference
LEFT  JOIN spy_sales_order_totals sot
      ON so.id_sales_order = sot.fk_sales_order
LEFT  JOIN spy_sales_expense se
    ON so.id_sales_order = se.fk_sales_order
LEFT  JOIN spy_sales_order_address soa
    ON so.fk_sales_order_address_billing = soa.id_sales_order_address
--WHERE date(so.created_at) = '2023-02-22';


##########################
#DUMP RESTORATION QUERIES#
##########################

select CAST(fk_country	as INT64),
       CAST(fk_customer	as INT64),
       CAST(id_sales_order	as INT64),
       CASE WHEN b.fk_customer is NULL THEN FALSE ELSE TRUE END is_test,
       order_reference,
       CAST(fk_locale	as INT64),
       cart_note,
       currency_iso_code,
       CAST(order_exchange_rate	as FLOAT64),
       order_custom_reference,
       a.customer_reference,
       CAST(oms_processor_identifier	as INT64),
       CAST(id_sales_order_totals	as INT64),
       CAST(discount_total	as FLOAT64),
       CAST(grand_total	as FLOAT64),
       CAST(order_expense_total	as FLOAT64),
       CAST(refund_total	as FLOAT64),
       CAST(subtotal	as FLOAT64),
       CAST(tax_total	as FLOAT64),
       CAST(id_sales_expense	as INT64),
       CAST(discount_amount_aggregation	as FLOAT64),
       CAST(gross_price	as FLOAT64),
       name,
       CAST(net_price	as FLOAT64),
       CAST(price	as FLOAT64),
       CAST(price_to_pay_aggregation	as FLOAT64),
       CAST(refundable_amount	as FLOAT64),
       CAST(tax_amount	as FLOAT64),
       CAST(id_sales_order_address	as INT64),
       fk_region,
       address1,
       address2,
       address3,
       CAST(customer_created_at	as TIMESTAMP),
       CAST(created_at	as TIMESTAMP),
       4 inserted_at
  FROM aws_s3.sales_orders_dump a
  LEFT JOIN gcp_gs.test_fraud_users b USING (fk_customer);

select fk_sku_simple,
       merchant_id,
       CAST(fk_sales_order	as INT64),
       CAST(id_sales_order_item	as INT64),
       fk_sales_order_item_bundle,
       CAST(fk_sales_shipment	as INT64),
       CAST(quantity	as INT64),
       CASE WHEN is_quantity_splittable='1'THEN TRUE ELSE FALSE END,
       CAST(canceled_amount	as FLOAT64),
       CAST(discount_amount_aggregation	as FLOAT64),
       CAST(discount_amount_full_aggregation	as FLOAT64),
       CAST(gross_price	as FLOAT64),
       CAST(net_price	as FLOAT64),
       CAST(price	as FLOAT64),
       CAST(price_to_pay_aggregation	as FLOAT64),
       product_offer_reference,
       CAST(refundable_amount	as FLOAT64),
       CAST(product_option_price_aggregation	as FLOAT64),
       CAST(subtotal_aggregation	as FLOAT64),
       CAST(tax_amount	as FLOAT64),
       CAST(tax_amount_full_aggregation	as FLOAT64),
       CAST(created_at	as TIMESTAMP),
       4 inserted_at
  FROM aws_s3.sales_order_items_dump;

select CAST(id_sales_order_item_state AS INT64),
       CAST(fk_sales_order AS INT64),
       CAST(fk_sales_order_item	AS INT64),
       CAST(created_at AS TIMESTAMP),
       CAST(updated_at AS TIMESTAMP),
       3 inserted_at
FROM aws_s3.sales_order_item_states_dump
WHERE  DATE(updated_at)='2023-02-22';