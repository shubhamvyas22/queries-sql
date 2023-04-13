WITH fulfillments_per_order AS (
  SELECT order_id, count(1) as number_of_fulfillments
  FROM shipping.fulfillment_facts ff
  GROUP BY 1
), shops_gateways_gmv AS (
  SELECT DATE(DATE_TRUNC(reported_at, month)) as reported_date,
         SUM(gmv.transaction_amount_usd) AS transaction_amount_usd
    FROM finance.gmv_adjustment_facts AS gmv
      LEFT JOIN finance.payment_gateway_dimension AS gateways
        USING(_payment_gateway_key)
      LEFT JOIN finance.shop_dimension AS shops
        USING (_shop_key)
  WHERE gmv.reported_gmv_inclusion_status = 'Included in Reported GMV'
    AND DATE(gmv.reported_at) > DATE('2021-12-31')
    AND gmv.transaction_index = '1'
    AND shops.shop_country_code IN ("IN")
    AND transaction_amount_usd <= 1000000
    AND gmv.order_id IN (SELECT order_id FROM fulfillments_per_order)
    and gateways.payment_gateway_name = "Cash on Delivery (COD)"
    and DATE(DATE_TRUNC(reported_at, month)) < date_trunc(current_date(), month)
  GROUP BY 1
  order by 1
)
select t1.*, t2.brokered_gmv, t3.total_non_shopify_gmv_usd from shops_gateways_gmv t1
left join 
(SELECT
  DATE(date_trunc(month,month)) as reported_date,
  SUM(channel_gmv_usd) as brokered_gmv
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
  and is_shopify_gmv = True
  and DATE(date_trunc(month,month)) < date_trunc(current_date(), month)
GROUP BY
  1
ORDER BY
 1) t2
 on t1.reported_date = t2.reported_date
 left join (select date_trunc(date,month) as reported_date,
  
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
  and DATE(date_trunc(date,month)) < date_trunc(current_date(), month)
GROUP BY
  1
  order by 1) t3
  on t3.reported_date = t1.reported_date
