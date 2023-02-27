WITH cats AS (
SELECT  id_category      id_product_category,
        category_name_en
  FROM  aws_s3.catalog_products
 GROUP  BY 1,2
)

SELECT  a.id_product_category  id_product_category,
        STRING_AGG(b.category_l1_name_en, "|") category_L1_name_en,
        STRING_AGG(a.category_name_en, "|") category_name_en
  FROM  cats a
  LEFT  JOIN gcp_gs.map_product_categories b
        ON a.id_product_category = CAST(b.id_product_category as INT64)
 GROUP  BY 1