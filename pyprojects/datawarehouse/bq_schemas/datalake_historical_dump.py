#sales_order_item_state
SELECT  sooish.fk_oms_order_item_state id_sales_order_item_state,
        fk_sales_order,
        fk_sales_order_item,
        soi.created_at,
        sooish.created_at updated_at
  FROM  spy_oms_order_item_state_history sooish
  LEFT  JOIN spy_sales_order_item soi
ON soi.id_sales_order_item = sooish.fk_sales_order_item
WHERE sooish.created_at >= '2023-02-10';

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
WHERE cretaed_at >= '2023-02-10';

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
    ON so.fk_sales_order_address_billing = soa.id_sales_order_address\
WHERE so.cretaed_at >= '2023-02-10';