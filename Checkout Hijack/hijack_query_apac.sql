SELECT
  t1.api_client_id,
  acd.app_name,
  acd.app_type,
  acd.name,
  acd.channel_provider_name,
  shops.current_funnel_state,
  shops.current_merchant_active_status,
  shops.current_merchant_deal_type,
  pmd.current_merchant_success_manager,
  shops.current_mrr_band_ranked_range,
  DATE_TRUNC(date, MONTH) AS month,
  partner.partner_company_name, 
  partner.shopify_partner_id, 
  partner.partner_url,
  shops.shop_id,
  shops.shop_name,
  shops.shop_permanent_domain,
  shops.shop_storefront_url,
  acd.handle,
  acd.app_handle,
  shops.shop_country_code,
  CASE
    WHEN partner.shopify_partner_id IS NULL THEN TRUE
  ELSE
  FALSE
END
  AS is_api_developed_by_a_shop,
  CASE
    WHEN partner.shopify_partner_id IS NULL THEN acd.application_developer_shop_id
  ELSE
  partner.shopify_partner_id
END
  AS partner_shop_id,
  CASE
    WHEN partner.shopify_partner_id IS NULL THEN shops2.current_merchant_name
  ELSE
  partner.partner_name
END
  AS partner_merchant_name,
  SUM(total_shopify_gmv_usd) AS total_shopify_gmv_usd,
  SUM(total_non_shopify_gmv_usd) AS total_non_shopify_gmv_usd,
  SUM(total_gmv_usd) AS total_gmv_usd
FROM
  `shopify-data-bigquery-global.seamster_backroom_core_build.checkout_hijack_shop_custom_app_gmv_daily` t1
LEFT JOIN
  finance.shop_dimension AS shops
USING
  (_shop_key)
LEFT JOIN plus.merchant_dimension pmd 
on shops.current_merchant_id = pmd.merchant_id
LEFT JOIN
  channels.api_client_dimension AS acd
ON
  t1._api_client_key = acd._api_client_key
LEFT JOIN
  partnerships.partner_dimension AS partner
ON
  partner.shopify_partner_id = acd.partner_id
LEFT JOIN
  finance.shop_dimension AS shops2
ON
  shops2.shop_id = acd.application_developer_shop_id
WHERE
  shops.shop_country_code IN ('IN',
    'TH',
    'PH',
    'SG',
    'VN',
    'ID',
    'BN',
    'KH',
    'TL',
    'MY',
    'LA',
    'MM')
  AND DATE(date) > DATE('2020-12-31')
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  15,
  16,
  17,18,19,20,21,22,23,24