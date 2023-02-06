create table if not aws_s3.exists dim_products
(
    sku_id                       STRING,
    concrete_sku                 STRING,
    category_id                  STRING,
    category_name                STRING,
    brand_name                   STRING,
    product_name                 STRING,
    product_description          STRING,
    available_quantity           STRING,
    concrete_product_name        STRING,
    concrete_product_active      STRING,
    concrete_product_description STRING,
    concrete_price               STRING,
    gross_default_price          STRING,
    gross_original_price         STRING,
    net_default_price            STRING,
    net_original_price           STRING,
    abstract_product_url         STRING,
    image_url_large              STRING,
    image_url_small              STRING,
    merchant_name                STRING,
    product_attributes           STRING,
    inserted_at                  TIMESTAMP
);


create table if not exists aws_s3.sales_order_item_states
(
    id_sales_order_item_state INT64      not null,
    fk_sales_order            INT64      not null,
    id_sales_order_item       INT64      not null,
    created_at                TIMESTAMP,
    updated_at                TIMESTAMP,
    inserted_at               TIMESTAMP
)
    partition by DATE_TRUNC(created_at, MONTH)
    cluster by id_sales_order_item_state, fk_sales_order, id_sales_order_item
    options (require_partition_filter = TRUE);


create table if not exists aws_s3.sales_order_items
(
    sku                              STRING not null,
    merchant_id                      STRING not null,
    fk_sales_order                   INT64  not null,
    id_sales_order_item              INT64  not null,
    fk_sales_order_item_bundle       STRING,
    fk_sales_shipment                INT64,
    group_key                        STRING,
    quantity                         INT64,
    is_quantity_splittable           BOOL,
    canceled_amount                  INT64,
    discount_amount_aggregation      INT64,
    discount_amount_full_aggregation INT64,
    gross_price                      INT64,
    net_price                        INT64,
    price                            INT64,
    price_to_pay_aggregation         INT64,
    product_offer_reference          STRING,
    refundable_amount                INT64,
    product_option_price_aggregation INT64,
    subtotal_aggregation             INT64,
    tax_amount                       INT64,
    tax_amount_full_aggregation      INT64,
    created_at                       TIMESTAMP,
    inserted_at                      TIMESTAMP
)
    partition by DATE_TRUNC(created_at, MONTH)
    cluster by sku, merchant_id, fk_sales_order, id_sales_order_item
    options (require_partition_filter = TRUE);


create table if not exists aws_s3.sales_order_states
(
    id_sales_order_state      INT64 not null,
    id_sales_order            INT64 not null,
    created_at                TIMESTAMP,
    updated_at                TIMESTAMP,
    inserted_at               TIMESTAMP
)
    partition by DATE_TRUNC(created_at, MONTH)
    cluster by id_order_state, id_sales_order
    options (require_partition_filter = TRUE);


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
    price_mode                  STRING,
    order_exchange_rate         INT64,
    order_custom_reference      STRING,
    customer_reference          STRING,
    oms_processor_identifier    INT64,
    id_sales_order_totals       INT64,
    canceled_total              INT64,
    discount_total              INT64,
    grand_total                 INT64,
    order_expense_total         INT64,
    refund_total                INT64,
    subtotal                    INT64,
    tax_total                   INT64,
    id_sales_expense            INT64,
    discount_amount_aggregation INT64,
    gross_price                 INT64,
    name                        STRING,
    net_price                   INT64,
    price                       INT64,
    price_to_pay_aggregation    INT64,
    refundable_amount           INT64,
    tax_amount                  INT64,
    id_sales_order_address      INT64,
    fk_region                   STRING,
    address1                    STRING,
    address2                    STRING,
    address3                    STRING,
    created_at                  TIMESTAMP,
    inserted_at                 TIMESTAMP
)

    partition by DATE_TRUNC(created_at, MONTH)
    cluster by fk_country, fk_customer, id_sales_order
    options (require_partition_filter = TRUE);

create table if not exists etl_metadata.airflow_run
(
    pipeline_nk STRING(50) not null options (description ='pipeline natural key'),
    delta       FLOAT64    not null options (description ='last pipeline airflow run timestamp'),
    inserted_at TIMESTAMP
)
    cluster by pipeline_nk;

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

create table if not exists gcp_gs.map_order_status
(
    id_oms_order_item_state STRING,
    spryker_order_state     STRING,
    reporting_order_state   STRING,
    inserted_at             TIMESTAMP
);







