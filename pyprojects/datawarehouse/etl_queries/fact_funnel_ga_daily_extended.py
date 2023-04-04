WITH event_stg AS (
    SELECT  DATE(DATE_ADD(event_timestamp, INTERVAl 3 HOUR)) event_date_nk,
            ga_session_id,
            user_pseudo_id,
            COALESCE(platform,'undefined')                   platform,
            COALESCE(name,'undefined')                       name,
            COALESCE(campaign,'undefined')                   campaign,
            COALESCE(campaign_id,'undefined')                campaign_id,
            COALESCE(medium,'undefined')                     medium,
            COALESCE(source,'undefined')                     source
      FROM  gcp_ga.{event_name}
     WHERE  DATE(event_timestamp) >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)), INTERVAL {incr_interval})
)

SELECT  event_date_nk,
        '{event_name}'                            event_name,
        platform,
        name                                      channel,
        campaign_id,
        campaign,
        medium,
        source,
        COUNT(user_pseudo_id)                     number_of_users,
        COUNT(DISTINCT user_pseudo_id)            number_of_unique_users,
        COUNT(ga_session_id)                      number_of_sessions,
        COUNT(DISTINCT ga_session_id)             number_of_unique_sessions
  FROM  event_stg
 GROUP  BY 1,3,4,5,6,7,8