SELECT  id_category       fk_product_category,
        id_sku_simple     sku,
        merchant_name_en,
        brand_name_en     brand,
        simple_name_en,
        category_name_en,
        if_simple_active if_sku_active
  FROM  aws_s3.catalog_products