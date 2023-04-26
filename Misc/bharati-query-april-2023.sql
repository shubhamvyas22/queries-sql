SELECT
  t2.commercial_subregion_code,
  t1.latest_annualized_gmv_band, 
  t1.country_name,
  sum(t3.annual_gmv) as region_gmv,
  count(t1._merchant_key) as merchant_count,
  count(t1.current_shop_count) as shop_count
FROM
  plus.plus_merchant_dimension t1
LEFT JOIN
  revenue.commercial_region_mapping_dimension t2
ON
  t1.country_code = t2.country_code
LEFT JOIN (
  SELECT
    _merchant_key,
    SUM(gmv) AS annual_gmv
  FROM
    `seamster_backroom_revenue.revenue_merchant_daily_periodic_snapshot`
  WHERE
    date > DATE_SUB(CURRENT_DATE(), INTERVAL 1 year)
  GROUP BY
    1 ) t3
ON
  t1._merchant_key = t3._merchant_key
WHERE
t1.geographic_region_code = "APAC"
and t1.is_included_in_historical_reporting = True
and t1.is_active = True
group by 1,2,3 