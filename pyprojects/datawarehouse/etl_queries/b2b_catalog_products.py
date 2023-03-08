WITH products AS (
SELECT  MAX(pa.abstract_sku)                            id_sku_config,
        pc.concrete_sku                                 id_sku_simple,
        MAX(pa.category_key)                            id_category,
        MAX(c.name_en__us)                              category_name_en,
        MAX(pa.value_1_en__us)                          brand_name_en,
        MAX(pa.name_en__us)                             config_name_en,
        MAX(pc.name_en__us)                             simple_name_en,
        SUM(COALESCE(s.quantity,0))                     simple_quantity,
        CASE WHEN MAX(pbaas.approval_status) = 'approved'
             THEN 1 ELSE 0 END                          if_simple_active,
        STRING_AGG(CAST(p.value_net as STRING), "@")    net_price,
        STRING_AGG(CAST(p.value_gross as STRING), "@")  gross_price,
        MAX(pa.value_1_en__us)                          merchant_name_en
  FROM  aws_s3.b2b_product_abstract pa
  LEFT  JOIN aws_s3.b2b_product_concrete pc
        USING (abstract_sku)
  LEFT  JOIN aws_s3.b2b_product_abstract_approval_status pbaas
        ON pbaas.sku = pa.abstract_sku
  LEFT  JOIN aws_s3.b2b_product_stock s
        USING (concrete_sku)
  LEFT  JOIN aws_s3.b2b_product_price p
        USING (concrete_sku)
  LEFT  JOIN aws_s3.b2b_category c
        USING (category_key)
 GROUP  BY 2)

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
        COALESCE(CAST(SPLIT(net_price,'@')[OFFSET(0)] AS FLOAT64), 0) net_default_price,
        COALESCE(CAST(SPLIT(net_price,'@')[OFFSET(1)] AS FLOAT64), 0) net_original_price,
        merchant_name_en
  FROM  products