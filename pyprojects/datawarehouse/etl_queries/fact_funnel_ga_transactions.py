WITH orders AS (
    SELECT  DATE(DATE_ADD(MIN(event_timestamp), INTERVAl 3 HOUR)) event_date_nk,
            MIN(ga_session_id)                                    ga_session_id,
            MIN(user_pseudo_id)                                   user_pseudo_id,
            COALESCE(MIN(platform),'undefined')                   platform,
            COALESCE(MIN(name),'undefined')                       name,
            COALESCE(MIN(medium),'undefined')                     medium,
            COALESCE(MIN(source),'undefined')                     source,
            order_id                                         order_reference
      FROM  gcp_ga.`Order`
     WHERE  DATE(event_timestamp) >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)), INTERVAL {incr_interval})
     GROUP  BY 8
),

purchases AS (
    SELECT  DATE(DATE_ADD(MIN(event_timestamp), INTERVAl 3 HOUR)) event_date_nk,
            MIN(ga_session_id)                                    ga_session_id,
            MIN(user_pseudo_id)                                   user_pseudo_id,
            COALESCE(MIN(platform),'undefined')                   platform,
            COALESCE(MIN(name),'undefined')                       name,
            COALESCE(MIN(medium),'undefined')                     medium,
            COALESCE(MIN(source),'undefined')                     source,
            transaction_id                                        order_reference
      FROM  gcp_ga.purchase
     WHERE  DATE(event_timestamp) >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)), INTERVAL {incr_interval})
     GROUP  BY 8
)

SELECT  COALESCE(a.event_date_nk, b.event_date_nk)      event_date_nk,
        COALESCE(a.order_reference, b.order_reference)  order_reference,
        COALESCE(a.ga_session_id, b.ga_session_id)      ga_session_id,
        COALESCE(a.user_pseudo_id, b.user_pseudo_id)     user_pseudo_id,
        COALESCE(a.platform, b.platform)                platform,
        COALESCE(a.name, b.name)                        channel,
        COALESCE(a.medium, b.medium)                    medium,
        COALESCE(a.source, b.source)                    source,
        CASE WHEN b.order_reference IS NOT NULL THEN 1 else 0 END if_order
  FROM  purchases a LEFT JOIN  orders b
        USING(order_reference)