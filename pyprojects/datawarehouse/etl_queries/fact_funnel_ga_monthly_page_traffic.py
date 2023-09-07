WITH event_stg AS (
    SELECT  DATE(DATE_ADD(event_timestamp, INTERVAl 3 HOUR)) event_date_nk,
            ga_session_id,
            user_pseudo_id,
            LEFT(page_location, 60)                          reduced_page_location
      FROM  gcp_ga.{event_name}
     WHERE  DATE(DATE_TRUNC(DATE_ADD(event_timestamp, INTERVAl 3 HOUR), MONTH))>=
                    DATE_SUB(DATE(DATE_TRUNC(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR), MONTH)), INTERVAL {incr_interval})
)

SELECT  DATE_TRUNC(estg.event_date_nk, MONTH)  event_date_nk,
        '{event_name}'                         event_name,
        LOWER(molg.page_location)              page_location,
        COUNT(estg.user_pseudo_id)             number_of_users,
        COUNT(DISTINCT estg.user_pseudo_id)    number_of_unique_users,
        COUNT(estg.ga_session_id)              number_of_sessions,
        COUNT(DISTINCT estg.ga_session_id)     number_of_unique_sessions
  FROM  event_stg estg
  LEFT  JOIN gcp_gs.map_{business_type}_page_location_groups molg
        ON LOWER(estg.reduced_page_location) LIKE CONCAT('%',LOWER(molg.page_location),'%')
 GROUP  BY 1,3