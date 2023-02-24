SELECT  MAX(id_category)       fk_product_category,
        id_sku_simple          sku,
        MAX(merchant_name_en)  merchant_name_en,
        MAX(brand_name_en)     brand,
        MAX(simple_name_en)    simple_name_en,
        MAX(category_name_en)  category_name_en,
        MAX(if_simple_active)  if_sku_active
  FROM  aws_s3.catalog_products
 GROUP  BY 2