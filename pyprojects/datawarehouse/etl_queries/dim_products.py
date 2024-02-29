SELECT  MAX(id_category)                      fk_product_category,
        COALESCE(id_sku_simple, 'undefined')  sku,
        MAX(merchant_name_en)                 merchant_name_en,
        MAX(brand_name_en)                    brand,
        MAX(simple_name_en)                   simple_name_en,
        MAX(if_simple_active)                 if_sku_active
  FROM  aws_s3.{0}_catalog_products
 GROUP  BY 2
UNION ALL
SELECT  MAX(id_category)                      fk_product_category,
        COALESCE(id_sku_simple, 'undefined')  sku,
        MAX(merchant_name_en)                 merchant_name_en,
        MAX(brand_name_en)                    brand,
        MAX(simple_name_en)                   simple_name_en,
        MAX(CAST(if_simple_active AS INT64))  if_sku_active
  FROM  aws_s3.{0}_catalog_products_vendure
 GROUP  BY 2