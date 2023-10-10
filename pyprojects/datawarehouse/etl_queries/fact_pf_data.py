SELECT  extract(week from MAX(inserted_at))                                                    report_week_num,
        extract (year from MAX(inserted_at))                                                   report_year,
        MIN(DATE(inserted_at))                                                                 report_date,
        listing_nk,
        MIN(type_identifier)                                                                   type_name_en,
        MAX(bedroom_name)                                                                      type_l1_name_en,
        DATE_DIFF(CURRENT_DATE(), MIN(CAST(LEFT(date_insert,10) as DATE)), DAY)                listing_days_online,
        MAX(size)                                                                              size,
        MAX(bedroom_value)                                                                     bedrooms_num,
        MAX(bathroom_value)                                                                    bathrooms_num,
        AVG(CAST(default_price as DECIMAL))                                                    price_aed,
        COALESCE(MAX(NULLIF(completion_status,'nan')),'undefined')                             completion_status_en,
        IF(MAX(CAST(LEFT(REPLACE(delivery_date,'nan',null),10) as DATE)) is NULL,0,1)          if_expected_completion,
        MAX(CAST(LEFT(REPLACE(delivery_date,'nan',null),10) as DATE))                          expected_completion_date,
        CASE WHEN LOWER(MAX(verified)) = 'true' OR LOWER(MAX(verified_by_owner)) = 'true'
        THEN True ELSE False END                                                               if_verified,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(0)])                        city_name_en,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(1)])                        area_name_en,
        IF(LENGTH(MIN(location_tree_path))-LENGTH(REPLACE(MIN(location_tree_path),',',''))>1,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(2)]), 'undefined')          district_name_en,
        IF(LENGTH(MIN(location_tree_path))-LENGTH(REPLACE(MIN(location_tree_path),',',''))>2,
        REVERSE(SPLIT(REVERSE(MIN(location_tree_path)),',')[OFFSET(3)]), 'undefined')          project_name_en,
        MIN(share_url)                                                                         listing_url
  FROM  scrapers.bv
 WHERE  price_text='1'
        AND DATE_TRUNC(inserted_at, DAY) = DATE_TRUNC(CURRENT_TIMESTAMP(), DAY)
 GROUP  BY 4