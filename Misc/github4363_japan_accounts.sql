
select t1.shop_id, sum_gmv as annual_gmv, t3.*  from 
(SELECT  shop_id, sum(channel_gmv_usd) as sum_gmv FROM `shopify-data-bigquery-global.channels.channel_aggregated_gmv_monthly` where is_shopify_gmv = True
and extract(year from month_begin_date) = 2022
and shop_id in (select distinct shop_id from finance.shop_dimension 
where shop_country_code = "JP"
and current_plan_name !="shopify_plus"
and using_shopify_payments = "Is Using Shopify Payments"
)
group by 1) t1
left join `shopify-data-bigquery-global.revenue.shop_salesforce_account_lookup` t2
on t1.shop_id = t2.shop_id
left join `raw_salesforce_trident.from_longboat_accounts` t3 
on t2.account_id = t3.id
where sum_gmv > 1000000