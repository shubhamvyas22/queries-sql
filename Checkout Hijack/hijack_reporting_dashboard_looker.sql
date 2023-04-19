WITH
  Sales AS (
  SELECT
    DATE("2022-01-01") AS reported_at,
    0 AS brokered_projection,
    0 AS hijacked_projection,
    0 AS cod_projection,
    0 AS captured
  UNION ALL
  SELECT
    DATE("2022-02-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2022-02-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2022-03-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2022-04-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2022-05-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2022-06-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2022-07-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2022-08-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2022-09-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2022-10-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2022-11-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2022-12-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2023-01-01"),
    0,
    0,
    0,
    0
  UNION ALL
  SELECT
    DATE("2023-02-01"),
    84446445,
    24033694,
    59065915,
    0
  UNION ALL
  SELECT
    DATE("2023-03-01"),
    93901073,
    24985383,
    60302391,
    0
  UNION ALL
  SELECT
    DATE("2023-04-01"),
    82075949,
    26039039,
    61671347,
    0
  UNION ALL
  SELECT
    DATE("2023-05-01"),
    83577054,
    27058706,
    62996143,
    0
  UNION ALL
  SELECT
    DATE("2023-06-01"),
    83436793,
    28112362,
    64365099,
    0
  UNION ALL
  SELECT
    DATE("2023-07-01"),
    92167312,
    25507713,
    57831422,
    11482787
  UNION ALL
  SELECT
    DATE("2023-08-01"),
    85515810,
    24576550,
    51386096,
    23294124
  UNION ALL
  SELECT
    DATE("2023-09-01"),
    73854602,
    19913719,
    44319417,
    35434010
  UNION ALL
  SELECT
    DATE("2023-10-01"),
    88463550,
    16866887,
    37263473,
    47881249
  UNION ALL
  SELECT
    DATE("2023-11-01"),
    94910030,
    13696868,
    30064418,
    60672935
  UNION ALL
  SELECT
    DATE("2023-12-01"),
    87208680,
    10357814,
    22659495,
    73761375 )
SELECT
  *
FROM
  Sales t1
LEFT JOIN (
  SELECT
    *
  FROM (
    SELECT
      DISTINCT DATE_TRUNC(DATE(date), month) AS date_calculated
    FROM
      finance.date_dimension t1
    WHERE
      year IN ('2022',
        '2023')) t1
  LEFT JOIN (
    WITH
      fulfillments_per_order AS (
      SELECT
        order_id,
        COUNT(1) AS number_of_fulfillments
      FROM
        shipping.fulfillment_facts ff
      GROUP BY
        1 ),
      shops_gateways_gmv AS (
      SELECT
        DATE(DATE_TRUNC(reported_at, month)) AS reported_date,
        SUM(gmv.transaction_amount_usd) AS transaction_amount_usd
      FROM
        finance.gmv_adjustment_facts AS gmv
      LEFT JOIN
        finance.payment_gateway_dimension AS gateways
      USING
        (_payment_gateway_key)
      LEFT JOIN
        finance.shop_dimension AS shops
      USING
        (_shop_key)
      WHERE
        gmv.reported_gmv_inclusion_status = 'Included in Reported GMV'
        AND DATE(gmv.reported_at) > DATE('2021-12-31')
        AND gmv.transaction_index = '1'
        AND shops.shop_country_code IN ("IN")
        AND transaction_amount_usd <= 1000000
        AND gmv.order_id IN (
        SELECT
          order_id
        FROM
          fulfillments_per_order)
        AND gateways.payment_gateway_name = "Cash on Delivery (COD)"
        AND DATE(DATE_TRUNC(reported_at, month)) < DATE_TRUNC(CURRENT_DATE(), month)
      GROUP BY
        1
      ORDER BY
        1 )
    SELECT
      t1.*,
      t2.brokered_gmv,
      t3.total_non_shopify_gmv_usd
    FROM
      shops_gateways_gmv t1
    LEFT JOIN (
      SELECT
        DATE(DATE_TRUNC(month,month)) AS reported_date,
        SUM(channel_gmv_usd) AS brokered_gmv
      FROM
        `channels.channel_aggregated_gmv_daily`
      WHERE
        _shop_key IN (
        SELECT
          DISTINCT _shop_key
        FROM
          finance.shop_dimension
        WHERE
          shop_country_code = "IN")
        AND EXTRACT(year
        FROM
          month) IN (2022,
          2023)
        AND is_shopify_gmv = TRUE
        AND DATE(DATE_TRUNC(month,month)) < DATE_TRUNC(CURRENT_DATE(), month)
      GROUP BY
        1
      ORDER BY
        1) t2
    ON
      t1.reported_date = t2.reported_date
    LEFT JOIN (
      SELECT
        DATE_TRUNC(date,month) AS reported_date,
        SUM(total_non_shopify_gmv_usd) AS total_non_shopify_gmv_usd
      FROM
        `shopify-data-bigquery-global.seamster_backroom_core_build.checkout_hijack_shop_custom_app_gmv_daily` t1
      LEFT JOIN
        finance.shop_dimension AS shops
      USING
        (_shop_key)
      WHERE
        shops.shop_country_code IN ('IN')
        AND DATE(date) > DATE('2021-12-31')
        AND DATE(DATE_TRUNC(date,month)) < DATE_TRUNC(CURRENT_DATE(), month)
      GROUP BY
        1
      ORDER BY
        1) t3
    ON
      t3.reported_date = t1.reported_date) t2
  ON
    t2.reported_date = t1.date_calculated ) t2
ON
  t1.reported_at = t2.reported_date