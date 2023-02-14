WITH cats AS (
SELECT  id_category      id_product_category,
        category_name_en
  FROM  aws_s3.historical_catalog_products
 WHERE  DATE(inserted_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
 GROUP  BY 1,2
)

SELECT  id_product_category,
        STRING_AGG(category_name_en, "|") category_name_en
  FROM  cats
 GROUP  BY 1