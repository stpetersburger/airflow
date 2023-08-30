WITH orders AS (
    SELECT  DATE(DATE_ADD(MIN(event_timestamp), INTERVAl 3 HOUR)) event_date_nk,
            MIN(ga_session_id)                                    ga_session_id,
            MIN(user_pseudo_id)                                   user_pseudo_id,
            COALESCE(MIN(platform),'undefined')                   platform,
            COALESCE(MIN(name),'undefined')                       name,
            COALESCE(MIN(medium),'undefined')                     medium,
            COALESCE(MIN(source),'undefined')                     source,
            order_id                                              order_reference
      FROM  gcp_ga.`Order`
     WHERE  DATE(event_timestamp) >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)), INTERVAL {incr_interval})
            AND order_id IS NOT NULL
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
            AND transaction_id IS NOT NULL
     GROUP  BY 8
)

SELECT  COALESCE(MIN(a.event_date_nk), MIN(b.event_date_nk))    event_date_nk,
        COALESCE(a.order_reference, b.order_reference)          order_reference,
        COALESCE(MIN(a.ga_session_id), MIN(b.ga_session_id))    ga_session_id,
        COALESCE(MIN(a.user_pseudo_id), MIN(b.user_pseudo_id))  user_pseudo_id,
        COALESCE(MIN(a.platform), MIN(b.platform))              platform,
        COALESCE(MIN(a.name), MIN(b.name))                      channel,
        COALESCE(MIN(a.medium), MIN(b.medium))                  medium,
        COALESCE(MIN(a.source`), MIN(b.source))                 source,
        CASE WHEN MIN(b.order_reference) IS NOT NULL
             THEN 1 else 0
        END                                                     if_order
  FROM  purchases a LEFT JOIN  orders b
        USING(order_reference)
 GROUP  BY 2