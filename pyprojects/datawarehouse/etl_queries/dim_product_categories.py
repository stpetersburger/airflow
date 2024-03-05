WITH cats AS (
SELECT  id_category                  id_product_category,
        category_name_en
  FROM  aws_s3.{0}_catalog_products
 GROUP  BY 1,2
UNION ALL
SELECT  id_category                  id_product_category,
        category_name_en
  FROM  aws_s3.{0}_catalog_products_vendure
 GROUP  BY 1,2
)
SELECT  a.id_product_category                                           id_product_category,
        STRING_AGG(COALESCE(b.category_l1_name_en,
                            COALESCE(a.category_name_en,
                                     'unassigned')), "|")               category_L1_name_en,
        STRING_AGG(a.category_name_en, "|")                             category_name_en,
        MAX(COALESCE(CAST(base_influencer_commission AS FLOAT64),0))    base_influencer_commission
  FROM  cats a
  LEFT  JOIN gcp_gs.map_product_categories b
        ON CAST(a.id_product_category AS STRING) = CAST(b.id_product_category AS STRING)
        AND b.business_type = '{0}'
 GROUP  BY 1