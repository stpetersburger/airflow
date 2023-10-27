SELECT  DATE_ADD(MAX(inserted_at), INTERVAL 4 HOUR)                                           inserted_at,
        DATE(DATE_TRUNC(DATE_ADD(MAX(inserted_at), INTERVAL 4 HOUR), DAY))                    scrapping_date_nk,
        CAST(listing_nk AS INTEGER)                                                           listing_nk,
        MIN(type_identifier)                                                                  property_type_name_en,
        CASE WHEN CAST(NULLIF(MAX(bedroom_value),'nan') AS DECIMAL) = 0 THEN 'studio'
        ELSE
          CASE WHEN CAST(NULLIF(MAX(bedroom_value),'nan') AS DECIMAL) > 7 THEN '7+'
          ELSE MAX(bedroom_value)
          END
        END                                                                                   property_type_l1_name_en,
        DATE_ADD(MAX(CAST(date_insert AS TIMESTAMP)), INTERVAL 4 HOUR)                        listing_created_at,
        DATE(DATE_TRUNC(DATE_ADD(MAX(CAST(date_insert AS TIMESTAMP)), INTERVAL 4 HOUR), DAY)) listing_date_nk,
        COALESCE(MAX(CAST(NULLIF(NULLIF(size,''), 'nan') AS DECIMAL)),-9999)                  size,
        COALESCE(MAX(CAST(NULLIF(NULLIF(bedroom_value,''), 'nan') AS DECIMAL)),-9999)          bedrooms_num,
        COALESCE(MAX(CAST(NULLIF(NULLIF(bathroom_value,''),'nan') AS DECIMAL)),-9999)          bathrooms_num,
        AVG(CAST(default_price as DECIMAL))                                                   price_aed,
        COALESCE(MAX(NULLIF(completion_status,'nan')),'undefined')                            completion_status_en,
        CAST(MAX(verified) AS BOOL)                                                           if_verified,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(0)])                       city_name_en,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(1)])                       area_name_en,
        IF(LENGTH(MIN(location_tree_path))-LENGTH(REPLACE(MIN(location_tree_path),',',''))>1,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(2)]), 'undefined')         district_name_en,
        IF(LENGTH(MIN(location_tree_path))-LENGTH(REPLACE(MIN(location_tree_path),',',''))>2,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(3)]), 'undefined')         project_name_en,
        DATE_DIFF(MAX(inserted_at),MAX(CAST(date_insert AS TIMESTAMP)), DAY)                  listing_ageing_day,
        MIN(share_url)                                                                        listing_url,
        1                                                                                     listing_source_nk
  FROM  scrapers.tp
 WHERE  DATE_TRUNC(inserted_at, DAY) = DATE_TRUNC(CURRENT_TIMESTAMP(), DAY)
 GROUP  BY 3