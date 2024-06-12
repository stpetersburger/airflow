WITH event_stg AS (
    SELECT  DATE(DATE_ADD(event_timestamp, INTERVAl 3 HOUR)) event_date_nk,
            ga_session_id,
            user_pseudo_id,
            install_source,
            platform
      FROM  {schema}.{event_name}
     WHERE  DATE(DATE_ADD(event_timestamp, INTERVAl 3 HOUR)) >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)), INTERVAL {incr_interval})
),

event_stg1 AS (
SELECT  event_date_nk,
        COUNT(ga_session_id) OVER (PARTITION BY event_date_nk)            number_of_sessions_overall,
        COUNT(DISTINCT ga_session_id) OVER (PARTITION BY event_date_nk)   number_of_unique_sessions_overall,
        COUNT(DISTINCT user_pseudo_id) OVER (PARTITION BY event_date_nk)  number_of_unique_users_overall,
        install_source,
        platform,
        user_pseudo_id,
        ga_session_id,
  FROM  event_stg
)

SELECT  event_date_nk,
        '{event_name}'                            event_name,
        MIN(number_of_sessions_overall)           number_of_sessions_overall,
        MIN(number_of_unique_sessions_overall)    number_of_unique_sessions_overall,
        MIN(number_of_unique_users_overall)       number_of_unique_users_overall,
        install_source,
        platform,
        COUNT(user_pseudo_id)                     number_of_users,
        COUNT(DISTINCT user_pseudo_id)            number_of_unique_users,
        COUNT(ga_session_id)                      number_of_sessions,
        COUNT(DISTINCT ga_session_id)             number_of_unique_sessions
  FROM  event_stg1
 GROUP  BY 1,6,7