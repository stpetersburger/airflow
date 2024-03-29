drop table if exists aws_s3.catalog_products;
create table if not exists aws_s3.catalog_products
(
    id_sku_config          STRING,
    id_sku_simple          STRING,
    id_category            INT64,
    category_name_en       STRING,
    brand_name_en          STRING,
    config_name_en         STRING,
    simple_name_en         STRING,
    simple_quantity        INT64,
    if_simple_active       INT64,
    simple_price           FLOAT64,
    gross_default_price    FLOAT64,
    gross_original_price   FLOAT64,
    net_default_price      FLOAT64,
    net_original_price     FLOAT64,
    merchant_name_en       STRING,
    inserted_at            TIMESTAMP
)
    cluster by id_sku_config, id_sku_simple, id_category, brand_name_en;

drop table if exists aws_s3.historical_catalog_products;
create table if not exists aws_s3.historical_catalog_products
(
    id_sku_config          STRING,
    id_sku_simple          STRING,
    id_category            INT64,
    category_name_en       STRING,
    brand_name_en          STRING,
    if_simple_active       INT64,
    gross_default_price    FLOAT64,
    gross_original_price   FLOAT64,
    net_default_price      FLOAT64,
    net_original_price     FLOAT64,
    merchant_name_en       STRING,
    inserted_at            TIMESTAMP
)
    partition by DATE_TRUNC(inserted_at, MONTH)
    cluster by id_sku_config, id_sku_simple, id_category, brand_name_en
    options (require_partition_filter = TRUE);

drop table if exists aws_s3.sales_order_item_states;
create table if not exists aws_s3.sales_order_item_states
(
    fk_sales_order_item_state INT64      not null,
    fk_sales_order            INT64      not null,
    fk_sales_order_item       INT64      not null,
    created_at                TIMESTAMP,
    updated_at                TIMESTAMP,
    inserted_at               FLOAT64
)
    partition by DATE_TRUNC(created_at, MONTH)
    cluster by fk_sales_order_item_state, fk_sales_order, fk_sales_order_item
    options (require_partition_filter = TRUE);


drop table if exists aws_s3.sales_order_items;
create table if not exists aws_s3.sales_order_items
(
    fk_sku_simple                    STRING not null,
    merchant_id                      STRING not null,
    fk_sales_order                   INT64  not null,
    id_sales_order_item              INT64  not null,
    fk_sales_order_item_bundle       STRING,
    fk_sales_shipment                INT64,
    quantity                         INT64,
    is_quantity_splittable           BOOL,
    canceled_amount                  FLOAT64,
    discount_amount_aggregation      FLOAT64,
    discount_amount_full_aggregation FLOAT64,
    gross_price                      FLOAT64,
    net_price                        FLOAT64,
    price                            FLOAT64,
    price_to_pay_aggregation         FLOAT64,
    product_offer_reference          STRING,
    refundable_amount                FLOAT64,
    product_option_price_aggregation FLOAT64,
    subtotal_aggregation             FLOAT64,
    tax_amount                       FLOAT64,
    tax_amount_full_aggregation      FLOAT64,
    created_at                       TIMESTAMP,
    inserted_at                      FLOAT64
)
    partition by DATE_TRUNC(created_at, MONTH)
    cluster by fk_sku_simple, merchant_id, fk_sales_order, id_sales_order_item
    options (require_partition_filter = TRUE);


drop table if exists aws_s3.sales_orders;
create table if not exists aws_s3.sales_orders
(
    fk_country                  INT64   not null,
    fk_customer                 INT64   not null,
    id_sales_order              INT64   not null,
    is_test                     BOOL,
    order_reference             STRING,
    fk_locale                   INT64,
    cart_note                   STRING,
    currency_iso_code           STRING,
    order_exchange_rate         FLOAT64,
    order_custom_reference      STRING,
    customer_reference          STRING,
    oms_processor_identifier    INT64,
    fk_sales_order_totals       INT64,
    discount_total              FLOAT64,
    grand_total                 FLOAT64,
    order_expense_total         FLOAT64,
    refund_total                FLOAT64,
    subtotal                    FLOAT64,
    tax_total                   FLOAT64,
    fk_sales_expense            INT64,
    discount_amount_aggregation FLOAT64,
    gross_price                 FLOAT64,
    name                        STRING,
    net_price                   FLOAT64,
    price                       FLOAT64,
    price_to_pay_aggregation    FLOAT64,
    refundable_amount           FLOAT64,
    tax_amount                  FLOAT64,
    fk_sales_order_address      INT64,
    fk_region                   STRING,
    address1                    STRING,
    address2                    STRING,
    address3                    STRING,
    customer_created_at         TIMESTAMP,
    created_at                  TIMESTAMP,
    inserted_at                 FLOAT64
)

    partition by DATE_TRUNC(created_at, MONTH)
    cluster by fk_country, fk_customer, id_sales_order
    options (require_partition_filter = TRUE);


drop table if exists etl_metadata.airflow_run;
create table if not exists etl_metadata.airflow_run
(
    id_pipeline STRING(50) not null options (description ='pipeline natural key'),
    delta       FLOAT64    not null options (description ='last pipeline airflow run timestamp'),
    inserted_at TIMESTAMP
)
    cluster by id_pipeline;

create table if not exists gcp_gs.etl_config
(
    if_valid    STRING,
    pipeline    STRING,
    url         STRING,
    tab         STRING,
    name        STRING,
    dwh_schema  STRING,
    inserted_at TIMESTAMP
);

### VENDURE

drop table if exists aws_s3.sales_order_item_states_vendure;
create table aws_s3.b2c_sales_order_item_states_vendure
(
    fk_sku_simple             INT64,
    quantity                  INT64,
    remained_quantity         INT64,
    created_at                TIMESTAMP,
    updated_at                TIMESTAMP,
    fk_sales_order            INT64,
    fk_sales_order_item_state STRING,
    inserted_at               INT64
)
    partition by DATE_TRUNC(created_at, MONTH)
    cluster by fk_sales_order_item_state, fk_sku_simple, fk_sales_order
    options (require_partition_filter = TRUE);

drop table if exists aws_s3.b2c_sales_order_items_vendure;
create table aws_s3.b2c_sales_order_items_vendure_
(
    sk_sku_simple      STRING,
    merchant_id        STRING,
    fk_sku_simple      INT64,
    quantity           INT64,
    gross_price        INT64,
    net_price          INT64,
    created_at         TIMESTAMP,
    fk_sales_order     INT64,
    discount_amount    INT64,
    discount_type      STRING,
    inserted_at        INT64
)
    partition by DATE_TRUNC(created_at, MONTH)
    cluster by fk_sku_simple, fk_sales_order
    options (require_partition_filter = TRUE);

drop table if exists aws_s3.b2c_sales_orders_vendure;
create table aws_s3.b2c_sales_orders_vendure
(
    id_sales_order      INT64,
    fk_locale           STRING,
    currency_iso_code   STRING,
    fk_parent_order     STRING,
    created_at          TIMESTAMP,
    order_reference     STRING,
    order_exchange_rate STRING,
    channel_name        STRING,
    points_redeemed     FLOAT64,
    fk_customer         INT64,
    customer_created_at TIMESTAMP,
    customer_reference  STRING,
    country_name        STRING,
    address1            STRING,
    address2            STRING,
    city_name           STRING,
    grand_total         INT64,
    tax_total           INT64,
    is_test             BOOL,
    inserted_at         INT64
)
    partition by DATE_TRUNC(created_at, MONTH)
    cluster by customer_reference, fk_parent_order, id_sales_order
    options (require_partition_filter = TRUE);









