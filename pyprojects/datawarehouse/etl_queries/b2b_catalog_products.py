WITH products AS (
SELECT  SPLIT(pdf.concrete_sku,'-')[OFFSET(0)]                  id_sku_config,
        pdf.concrete_sku                                        id_sku_simple,
        STRING_AGG(LOWER(pdf.category_name), '|')               id_category,
        STRING_AGG(pdf.category_name, '|')                      category_name_en,
        STRING_AGG(padf.attribute_value, '|')                   brand_name_en,
        STRING_AGG(concrete_product_name, '|')                  config_name_en,
        STRING_AGG(concrete_product_name, '|')                  simple_name_en,
        0                                                       simple_quantity,
        MAX(concrete_product_active)                            if_simple_active,
        STRING_AGG(CAST(pdf.offer_price_net as STRING), "@")    net_price,
        STRING_AGG(CAST(pdf.offer_price_gross as STRING), "@")  gross_price,
        STRING_AGG(CAST(pdf.merchant_ref as STRING), '|')       merchant_name_en,
  FROM  aws_s3.b2b_product_data_feed_EN pdf
  LEFT  JOIN aws_s3.b2b_product_attribute_data_feed_EN padf
        ON SPLIT(pdf.concrete_sku,'-')[OFFSET(0)] = padf.sku
        AND padf.attribute_name='brand'
 GROUP  BY 2
)

SELECT  id_sku_config,
        id_sku_simple,
        id_category,
        category_name_en,
        brand_name_en,
        config_name_en,
        simple_name_en,
        simple_quantity,
        if_simple_active,
        COALESCE(CAST(SPLIT(gross_price,'@')[OFFSET(0)] AS FLOAT64), 0) simple_price,
        COALESCE(CAST(SPLIT(gross_price,'@')[OFFSET(0)] AS FLOAT64), 0) gross_default_price,
        COALESCE(CAST(SPLIT(gross_price,'@')[OFFSET(1)] AS FLOAT64), 0) gross_original_price,
        COALESCE(CAST(SPLIT(net_price,'@')[OFFSET(0)] AS FLOAT64), 0)   net_default_price,
        COALESCE(CAST(SPLIT(net_price,'@')[OFFSET(1)] AS FLOAT64), 0)   net_original_price,
        COALESCE(merchant_name_en, brand_name_en)                       merchant_name_en
  FROM  products