 SELECT
  t1.*,
  t2.shop_name,
  t2.shop_storefront_url,
  t2.shop_domain,
  t2.shop_permanent_domain,
  t3.merchant_name,
  t3.current_msm_name,
  t4.checkout_one_checkouts_annualized,
  t4.checkout_classic_checkouts_annualized,
  t4.checkout_one_gmv_annualized,
  t4.checkout_classic_gmv_annualized,
  t5.merchant_gmv_usd_annualized,
  t5.hijack_gmv_annualized,
  t5.shopify_gmv_annualized,
  t6.shopify_gmv_annualized,
  t6.gpv_annualized

FROM (
  SELECT
    DISTINCT _merchant_key,
    shop_id
  FROM
    `finance.shop_merchant_lookup`
  WHERE
    _merchant_key IN (
    SELECT
      DISTINCT _merchant_key
    FROM
      `plus.plus_merchant_dimension`
    WHERE
      country_code = 'IN'
      AND is_active = TRUE)) t1
LEFT JOIN (
  SELECT
    _shop_key,
    shop_id,
    shop_name,
    shop_storefront_url,
    shop_domain,
    shop_permanent_domain
  FROM
    `shopify-data-bigquery-global.finance.shop_dimension`
  WHERE
    shop_id IN (
    SELECT
      DISTINCT shop_id
    FROM
      `finance.shop_merchant_lookup`
    WHERE
      _merchant_key IN (
      SELECT
        DISTINCT _merchant_key
      FROM
        `plus.plus_merchant_dimension`
      WHERE
        country_code = 'IN'
        AND is_active = TRUE) )) t2
ON
  t1.shop_id = t2.shop_id
LEFT JOIN (
  SELECT
    _merchant_key,
    merchant_name,
    current_msm_name
  FROM
    `plus.plus_merchant_dimension`
  WHERE
    country_code = 'IN'
    AND is_active = TRUE) t3
ON
  t1._merchant_key = t3._merchant_key
LEFT JOIN (
  SELECT
    shop_id,
    SUM(checkout_one_checkouts) AS checkout_one_checkouts_annualized,
    SUM(checkout_classic_checkouts) AS checkout_classic_checkouts_annualized,
    SUM(checkout_one_gmv) AS checkout_one_gmv_annualized,
    SUM(checkout_classic_gmv) AS checkout_classic_gmv_annualized
  FROM
    `seamster_backroom_checkout.checkout_shop_daily_snapshot`
  WHERE
    date> DATE(2022, 3,29)
    AND shop_id IN (
    SELECT
      DISTINCT shop_id
    FROM
      `finance.shop_merchant_lookup`
    WHERE
      _merchant_key IN (
      SELECT
        DISTINCT _merchant_key
      FROM
        `plus.plus_merchant_dimension`
      WHERE
        country_code = 'IN'
        AND is_active = TRUE) )
  GROUP BY
    1) t4
ON
  t1.shop_id = t4.shop_id
LEFT JOIN (
  SELECT
    shop_id,
    SUM(total_gmv_usd) AS merchant_gmv_usd_annualized,
    SUM(total_non_shopify_gmv_usd) AS hijack_gmv_annualized,
    SUM(total_shopify_gmv_usd) AS shopify_gmv_annualized
  FROM
    `shopify-data-bigquery-global.seamster_backroom_core_build.checkout_hijack_shop_custom_app_gmv_daily`
  WHERE
    date> DATE(2022, 3,29)
    AND shop_id IN (
    SELECT
      DISTINCT shop_id
    FROM
      `finance.shop_merchant_lookup`
    WHERE
      _merchant_key IN (
      SELECT
        DISTINCT _merchant_key
      FROM
        `plus.plus_merchant_dimension`
      WHERE
        country_code = 'IN'
        AND is_active = TRUE) )
  GROUP BY
    1) t5
ON
  t5.shop_id = t1.shop_id
LEFT JOIN (
  SELECT
    shop_id,
    SUM(daily_gmv_amount_usd) AS shopify_gmv_annualized,
    SUM(daily_gmv_amount_usd) AS gpv_annualized
  FROM
    `shopify-data-bigquery-global.finance.merchant_shop_daily_gmv`
  WHERE
    date> DATE(2022, 3,29)
    AND shop_id IN (
    SELECT
      DISTINCT shop_id
    FROM
      `finance.shop_merchant_lookup`
    WHERE
      _merchant_key IN (
      SELECT
        DISTINCT _merchant_key
      FROM
        `plus.plus_merchant_dimension`
      WHERE
        country_code = 'IN'
        AND is_active = TRUE) )
  GROUP BY
    1) t6
ON
  t1.shop_id = t6.shop_id
ORDER BY
  1
