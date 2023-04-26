SELECT
  merchant_name,
  t1.current_msm_name,
  t2.currently_associated_partner_name,
  DATE_DIFF(CURRENT_DATE(),t1.first_contract_start_date,month) AS age_in_months,
  latest_annualized_gmv_band
FROM
  `shopify-data-bigquery-global.plus.plus_merchant_dimension` t1
LEFT JOIN
  admin.shop_dimension t2
ON
  t1.current_primary_shop_id = t2.shop_id
WHERE
  country_name = "India"
  AND is_active = TRUE