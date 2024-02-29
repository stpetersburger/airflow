WITH catalog_stg AS (
SELECT  id_category                           fk_product_category,
        COALESCE(id_sku_simple, 'undefined')  sku,
        merchant_name_en,
        brand_name_en,
        simple_name_en,
        if_simple_active
  FROM  aws_s3.{0}_catalog_products
UNION ALL
SELECT  id_category                                 fk_product_category,
        COALESCE(id_sku_simple, 'undefined')        sku,
        merchant_name_en,
        brand_name_en,
        simple_name_en,
        CAST(CASE WHEN LENGTH(if_simple_active) >1
                  THEN '0' ELSE  if_simple_active
              END AS INT64)                         if_simple_active
  FROM  aws_s3.{0}_catalog_products_vendure
)

SELECT  MAX(fk_product_category)  fk_product_category,
        sku,
        MAX(merchant_name_en)     merchant_name_en,
        MAX(brand_name_en)        brand,
        MAX(simple_name_en)       simple_name_en,
        MAX(if_simple_active)     if_sku_active
  FROM  catalog_stg
 GROUP  BY 2