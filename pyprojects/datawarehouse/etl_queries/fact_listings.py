SELECT  DATE_ADD(MAX(inserted_at), INTERVAL 4 HOUR)                                           inserted_at,
        DATE_TRUNC(DATE_ADD(MAX(inserted_at), INTERVAL 4 HOUR), DAY)                          scrapping_date_nk,
        CAST(listing_nk AS INTEGER)                                                           listing_nk,
        MIN(type_identifier)                                                                  property_type_name_en,
        MAX(bedroom_name)                                                                     property_type_l1_name_en,
        DATE_ADD(MAX(CAST(date_insert AS TIMESTAMP)), INTERVAL 4 HOUR)                        listing_created_at,
        DATE_TRUNC(DATE_ADD(MAX(CAST(date_insert AS TIMESTAMP)), INTERVAL 4 HOUR), DAY)       listing_date_nk,
        MAX(CAST(size AS DECIMAL))                                                            size,
        MAX(CAST(NULLIF(bedroom_value, 'nan') AS DECIMAL))                                    bedrooms_num,
        MAX(CAST(NULLIF(bathroom_value,'nan') AS DECIMAL))                                    bathrooms_num,
        AVG(CAST(default_price as DECIMAL))                                                   price_aed,
        COALESCE(MAX(NULLIF(completion_status,'nan')),'undefined')                            completion_status_en,
        CASE WHEN LOWER(MAX(verified)) = 'true' OR LOWER(MAX(verified_by_owner)) = 'true'
        THEN True ELSE False END                                                              if_verified,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(0)])                       city_name_en,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(1)])                       area_name_en,
        IF(LENGTH(MIN(location_tree_path))-LENGTH(REPLACE(MIN(location_tree_path),',',''))>1,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(2)]), 'undefined')         district_name_en,
        IF(LENGTH(MIN(location_tree_path))-LENGTH(REPLACE(MIN(location_tree_path),',',''))>2,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(3)]), 'undefined')         project_name_en,
        DATE_DIFF(MAX(inserted_at),MAX(CAST(date_insert AS TIMESTAMP)), DAY)                  listing_ageing_day,
        MIN(share_url)                                                                        listing_url,
        0                                                                                     listing_source_nk                                                                                    listing_source
  FROM  scrapers.bv
 WHERE  price_text='1'
        AND DATE_TRUNC(inserted_at, DAY) = DATE_TRUNC(CURRENT_TIMESTAMP(), DAY)
 GROUP  BY 3