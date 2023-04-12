SELECT
  t1.shop_id,
  current_merchant_name,
  facebook_social_handle,
  instagram_social_handle,
  shop_company_name,
  shop_domain,
  shop_name,
  t2.shop_gmv
FROM
  `shopify-data-bigquery-global.finance.shop_dimension` t1
LEFT JOIN (
  SELECT
    shop_id,
    SUM(channel_gmv_usd) AS shop_gmv
  FROM
    `shopify-data-bigquery-global.channels.channel_aggregated_gmv_daily`
  WHERE
    DATE(date) > DATE(2021,12,31)
  GROUP BY
    1) t2
ON
  t1.shop_id = t2.shop_id
WHERE
  shop_country_code IN ("CN",
    "HK",
    "TW")
  AND _shop_key IN (
  SELECT
    DISTINCT _shop_key
  FROM
    finance.gmv_adjustment_facts
  WHERE
    DATE(reported_at) > DATE(2021,12,31) )
ORDER BY
  8 desc