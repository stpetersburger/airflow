WITH dates AS (
    SELECT *
      FROM UNNEST(
                  GENERATE_DATE_ARRAY('2022-01-01',
                                      CURRENT_DATE(),
                                      INTERVAL 1 DAY)
                 )                                     generated_dates
)

SELECT  generated_dates                         date_nk,
        FORMAT_DATE('%x',generated_dates)       date_nk_usa,
        FORMAT_DATE('%Y%m%d', generated_dates)  date_code,
        FORMAT_DATE('%Y', generated_dates)      date_year_num,
        FORMAT_DATE('%m', generated_dates)      date_month_num,
        FORMAT_DATE('%B', generated_dates)      date_month_name,
        FORMAT_DATE('%b', generated_dates)      date_month_name_short,
        FORMAT_DATE('%Y-%m', generated_dates)   year_month_nk,
        FORMAT_DATE('%Y%m', generated_dates)    year_month_code,
        FORMAT_DATE('%Y-%b', generated_dates)   year_month_name_short,
        FORMAT_DATE('%u', generated_dates)      date_week_num,
        FORMAT_DATE('%V', generated_dates)      date_week_num_iso
  FROM  dates