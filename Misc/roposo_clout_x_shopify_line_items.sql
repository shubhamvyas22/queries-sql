SELECT
  *
FROM
  raw_shopify.from_longboat_line_items t1
LEFT JOIN (
  SELECT
    order_id,
    SUM(gmv_adjustment_usd) AS order_gmv
  FROM
    `finance.gmv_adjustment_facts`
  GROUP BY
    1) t2
ON
  CAST(t1.order_id AS string) = t2.order_id
WHERE
  CAST(product_id AS String) IN (
  SELECT
    product_id
  FROM
    products.product_dimension
  WHERE
    shop_id IN (
    SELECT
      shop_id
    FROM (
      SELECT
        shop_id,
        SUM(net_install_count) AS install_count
      FROM
        partnerships.api_client_install_facts t1
      WHERE
        api_client_id = '1167982593'
      GROUP BY
        shop_id))
    AND product_vendor = "Roposo Clout" )