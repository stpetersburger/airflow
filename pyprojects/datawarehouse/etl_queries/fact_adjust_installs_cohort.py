WITH installs_all AS (
  SELECT  DATE(_created_at_)            date_nk,
          _os_name_                     os_name_en,
          CASE WHEN LOWER(_os_name_)='android'
               THEN _gps_adid_
               ELSE _idfv_
          END                           fk_user_id
    FROM  aws_s3.adjust_events
   WHERE  LOWER(_activity_kind_) like 'install%'
),

installs_overall AS (
  SELECT  DATE(_created_at_)            date_nk,
          _os_name_                     os_name_en,
          count(1)                      installs_overall
    FROM  aws_s3.adjust_events
   WHERE  LOWER(_activity_kind_) like 'install%'
   GROUP  BY 1,2
),

purchases_android AS (
  SELECT  DATE(_created_at_)              date_nk,
          'android'                       os_name_en,
          _gps_adid_                      fk_user_id,
          _revenue_usd_                   revenue_usd,
          SUM(_revenue_usd_) OVER (PARTITION BY _os_name_, DATE(_created_at_)) revenue_overall
    FROM  aws_s3.adjust_events
   WHERE  _event_name_ = 'Revenue'
          AND LOWER(_os_name_) = 'android'
),

purchases_ios AS (
  SELECT  DATE(_created_at_)        date_nk,
          'ios'                     os_name_en,
          _idfv_                    fk_user_id,
          _revenue_usd_             revenue_usd,
          SUM(_revenue_usd_) OVER (PARTITION BY _os_name_, DATE(_created_at_)) revenue_overall
    FROM  aws_s3.adjust_events
   WHERE  _event_name_ = 'Revenue'
          AND LOWER(_os_name_) = 'ios'
),

purchases_all AS (SELECT * FROM purchases_android UNION ALL SELECT * FROM purchases_ios)

SELECT  pa.date_nk                             date_nk,
        pa.os_name_en                          os_name_en,
        ROUND(pa.revenue_overall,2)            revenue_overall_amnt,
        i.date_nk                              install_date_nk,
        io.installs_overall                    installs_num,
        DATE_DIFF(pa.date_nk, i.date_nk, DAY)  days_since_install_num,
        COUNT(1)                               cohort_users_num,
        ROUND(SUM(revenue_usd),2)              cohort_revenue_amnt
  FROM  purchases_all pa
  LEFT  JOIN installs_all i
        USING (fk_user_id)
  LEFT  JOIN installs_overall io
        ON i.date_nk = io.date_nk
        AND i.os_name_en = io.os_name_en
 WHERE  pa.date_nk>='2022-01-01' AND i.date_nk>='2022-01-01'
 GROUP  BY 1,2,3,4,5