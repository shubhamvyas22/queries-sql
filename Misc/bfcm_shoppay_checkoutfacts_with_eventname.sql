SELECT
  t1.shop_id,
  t1.event_name,
  t2.is_pay_user_at_time_of_checkout,
  AVG(TIME_DIFF(TIME(t2.completed_at), TIME(t2.created_at), SECOND)) AS time_taken,
  COUNT(t1.checkout_token) AS total_checkouts
FROM
  `revenue.bfcm_checkout_events` t1
LEFT JOIN
  `checkout.checkout_facts` t2
ON
  t1.checkout_token = t2.checkout_token
GROUP BY
  1,
  2,
  3