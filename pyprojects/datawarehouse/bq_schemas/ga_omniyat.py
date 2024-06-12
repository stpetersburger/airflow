CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.user_engagement`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  campaign STRING,
  content STRING,
  engaged_session_event STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  gclid STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  term STRING,
  campaign_id STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);
CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.scroll`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  campaign STRING,
  content STRING,
  engaged_session_event STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  gclid STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  percent_scrolled STRING,
  term STRING,
  campaign_id STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);
CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.first_visit`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  engaged_session_event STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  campaign_id STRING,
  campaign STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);
CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.form_submit_button`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  campaign STRING,
  content STRING,
  engaged_session_event STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  gclid STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  term STRING,
  campaign_id STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);
CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.video_start`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  campaign STRING,
  engaged_session_event STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  gclid STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  term STRING,
  video_current_time STRING,
  video_duration STRING,
  video_percent STRING,
  video_provider STRING,
  video_title STRING,
  video_url STRING,
  visible STRING,
  content STRING,
  campaign_id STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);
CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.form_submit`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  campaign STRING,
  engaged_session_event STRING,
  form_destination STRING,
  form_id STRING,
  form_length STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  term STRING,
  content STRING,
  campaign_id STRING,
  gclid STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);
CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.session_start`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  engaged_session_event STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  campaign_id STRING,
  campaign STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);
CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.file_download`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  campaign STRING,
  engaged_session_event STRING,
  file_extension STRING,
  file_name STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  link_id STRING,
  link_text STRING,
  link_url STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  term STRING,
  campaign_id STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);
CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.form_start`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  campaign STRING,
  content STRING,
  engaged_session_event STRING,
  first_field_id STRING,
  first_field_name STRING,
  first_field_position STRING,
  first_field_type STRING,
  form_destination STRING,
  form_id STRING,
  form_length STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  gclid STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  term STRING,
  campaign_id STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);
CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.video_progress`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  campaign STRING,
  engaged_session_event STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  gclid STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  term STRING,
  video_current_time STRING,
  video_duration STRING,
  video_percent STRING,
  video_provider STRING,
  video_title STRING,
  video_url STRING,
  visible STRING,
  content STRING,
  campaign_id STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);
CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.click`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  campaign STRING,
  content STRING,
  engaged_session_event STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  gclid STRING,
  link_classes STRING,
  link_domain STRING,
  link_id STRING,
  link_url STRING,
  outbound STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  term STRING,
  campaign_id STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);
CREATE TABLE `omniyat-analytics-datalake.gcp_ga_com.page_view`
(
  inserted_at TIMESTAMP,
  event_timestamp TIMESTAMP,
  user_pseudo_id STRING,
  event_date INT64,
  category STRING,
  mobile_brand_name STRING,
  mobile_model_name STRING,
  mobile_marketing_name STRING,
  mobile_os_hardware_model STRING,
  operating_system STRING,
  operating_system_version STRING,
  vendor_id STRING,
  advertising_id STRING,
  language STRING,
  is_limited_ad_tracking STRING,
  time_zone_offset_seconds INT64,
  browser STRING,
  browser_version STRING,
  country STRING,
  city STRING,
  id STRING,
  version STRING,
  install_store STRING,
  install_source STRING,
  platform STRING,
  name STRING,
  medium STRING,
  source STRING,
  hostname STRING,
  campaign STRING,
  content STRING,
  engaged_session_event STRING,
  entrances STRING,
  ga_session_id STRING,
  ga_session_number STRING,
  gclid STRING,
  page_location STRING,
  page_referrer STRING,
  page_title STRING,
  term STRING,
  campaign_id STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, MONTH)
CLUSTER BY user_pseudo_id, ga_session_id, medium, source
OPTIONS(
  require_partition_filter=true
);

CREATE TABLE `omniyat-analytics-datalake.analytics.fact_funnel_ga_daily_omniyatcom`
(
  event_date_nk DATE NOT NULL,
  event_name STRING NOT NULL,
  number_of_sessions_overall INT64,
  number_of_unique_sessions_overall INT64,
  number_of_unique_users_overall INT64,
  install_source STRING,
  platform STRING,
  number_of_users INT64,
  number_of_unique_users INT64,
  number_of_sessions INT64,
  number_of_unique_sessions INT64
)
PARTITION BY DATE_TRUNC(event_date_nk, MONTH)
CLUSTER BY event_name;
CREATE TABLE `omniyat-analytics-datalake.analytics.fact_funnel_ga_monthly_extended_omniyatcom`
(
  event_date_nk DATE NOT NULL,
  event_name STRING NOT NULL,
  platform STRING,
  source STRING,
  medium STRING,
  traffic_name STRING,
  campaign_id STRING,
  campaign STRING,
  marketing_channel STRING,
  marketing_campaign STRING,
  number_of_users INT64,
  number_of_unique_users INT64,
  number_of_sessions INT64,
  number_of_unique_sessions INT64
)
PARTITION BY DATE_TRUNC(event_date_nk, MONTH)
CLUSTER BY event_name;
CREATE TABLE `omniyat-analytics-datalake.analytics.fact_funnel_ga_daily_extended_omniyatcom`
(
  event_date_nk DATE NOT NULL,
  event_name STRING NOT NULL,
  platform STRING,
  source STRING,
  medium STRING,
  traffic_name STRING,
  campaign_id STRING,
  campaign STRING,
  marketing_channel STRING,
  marketing_campaign STRING,
  number_of_users INT64,
  number_of_unique_users INT64,
  number_of_sessions INT64,
  number_of_unique_sessions INT64
)
PARTITION BY DATE_TRUNC(event_date_nk, MONTH)
CLUSTER BY event_name;
CREATE TABLE `omniyat-analytics-datalake.analytics.fact_funnel_ga_monthly_page_traffic_omniyatcom`
(
  event_date_nk DATE NOT NULL,
  event_name STRING NOT NULL,
  page_location STRING,
  number_of_sessions_overall INT64,
  number_of_users INT64,
  number_of_unique_users INT64,
  number_of_sessions INT64,
  number_of_unique_sessions INT64,
  page_location_type STRING
)
PARTITION BY DATE_TRUNC(event_date_nk, MONTH)
CLUSTER BY event_name, page_location;
CREATE TABLE `omniyat-analytics-datalake.analytics.fact_funnel_ga_monthly_omniyatcom`
(
  event_date_nk DATE NOT NULL,
  event_name STRING NOT NULL,
  number_of_sessions_overall INT64,
  number_of_unique_sessions_overall INT64,
  number_of_unique_users_overall INT64,
  install_source STRING,
  platform STRING,
  number_of_users INT64,
  number_of_unique_users INT64,
  number_of_sessions INT64,
  number_of_unique_sessions INT64
)
PARTITION BY DATE_TRUNC(event_date_nk, MONTH)
CLUSTER BY event_name;
CREATE TABLE `omniyat-analytics-datalake.analytics.fact_funnel_ga_daily_page_traffic_omniyatcom`
(
  event_date_nk DATE NOT NULL,
  event_name STRING NOT NULL,
  page_location STRING,
  number_of_sessions_overall INT64,
  number_of_users INT64,
  number_of_unique_users INT64,
  number_of_sessions INT64,
  number_of_unique_sessions INT64
)
PARTITION BY DATE_TRUNC(event_date_nk, MONTH)
CLUSTER BY event_name, page_location;