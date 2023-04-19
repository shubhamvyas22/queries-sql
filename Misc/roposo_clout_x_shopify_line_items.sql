select t1.*,t2.payment_gateway_category from (
  SELECT
    t1.created_at, t1.order_id, t1.shop_id,t1.title, t1.quantity, t1.price, t2.order_gmv,t2.payment_gateway_key,t1.price,t1.fulfillment_status
  FROM
    raw_shopify.from_longboat_line_items t1
  LEFT JOIN (
    SELECT
      t1.order_id,
      min(t1._payment_gateway_key) as payment_gateway_key,
      SUM(t1.gmv_adjustment_usd) AS order_gmv
    FROM
      `finance.gmv_adjustment_facts` t1
      where extract(year from reported_at) > 2021
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
      AND product_vendor = "Roposo Clout" )) t1
      left join `finance.payment_gateway_dimension` t2
      on t1.payment_gateway_key = t2._payment_gateway_key