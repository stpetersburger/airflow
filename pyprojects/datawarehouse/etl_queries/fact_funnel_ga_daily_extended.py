WITH event_stg AS (
    SELECT  DATE(DATE_ADD(event_timestamp, INTERVAl 3 HOUR)) event_date_nk,
            ga_session_id,
            user_pseudo_id,
            COALESCE(platform,'undefined')                   platform,
            COALESCE(name,'undefined')                       traffic_name,
            COALESCE(campaign,'undefined')                   campaign,
            COALESCE(campaign_id,'undefined')                campaign_id,
            COALESCE(medium,'undefined')                     medium,
            COALESCE(source,'undefined')                     source
      FROM  gcp_ga.{event_name}
     WHERE  DATE(event_timestamp) >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)), INTERVAL {incr_interval})
)

SELECT  estg.event_date_nk,
        '{event_name}'                                 event_name,
        estg.platform,
        estg.traffic_name,
        estg.campaign_id,
        estg.campaign,
        estg.medium,
        estg.source,
        mch.marketing_channel,
        mch.marketing_campaign
        COUNT(estg.user_pseudo_id)                     number_of_users,
        COUNT(DISTINCT estg.user_pseudo_id)            number_of_unique_users,
        COUNT(estg.ga_session_id)                      number_of_sessions,
        COUNT(DISTINCT estg.ga_session_id)             number_of_unique_sessions
  FROM  event_stg estg
  LEFT  JOIN gsp_gs.map_b2c_ga_marketing_channels mch USING(source,medium,traffic_name)

 GROUP  BY 1,3,4,5,6,7,8,9,10