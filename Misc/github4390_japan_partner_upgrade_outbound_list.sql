SELECT t1.*, t2.collaborators,
t2.became_customer_at,
t2.first_sale_at,
t2.current_plan_name,
t2.shop_internal_dashboard_url,
t2.shop_permanent_domain,
t2.primary_industry,
t2.apps_score,
t2.top_apps_installed,
t2.last_30d_online_gmv,
t2.last_90d_online_gmv,
t2.last_180d_online_gmv FROM (SELECT
    md.merchant_id,
    md.merchant_name
  , md.merchant_country_name  
  , pd.partner_name
  , pd.partner_country_name
  , pd.current_partner_manager
  , sd.eligible_for_shopify_payments
  , sd.using_shopify_payments 
  , f.transition_at AS Transition_Date
  , cast(sum(daily_gmv_usd) as int) as GMV_L12M
  , cast(sum(daily_gross_shopify_payments_revenue_amount_usd) as int) as Shopify_Payments_Revenue_L12M

FROM revenue.merchant_state_region_facts f
  JOIN funnel.merchant_dimension AS md 
    ON md._merchant_key = f._merchant_key
  JOIN revenue.commercial_region_mapping_dimension cmd
    ON cmd._commercial_region_mapping_key = f._commercial_region_mapping_key 
  JOIN funnel.partner_dimension AS pd 
    ON md.currently_associated_shopify_partner_id = pd.shopify_partner_id
  JOIN funnel.shop_dimension AS sd  
    ON md.current_merchant_primary_shop_id = sd.shop_id
  JOIN funnel.shop_state_daily_periodic_snapshot_unresolved ssd 
    ON md.current_merchant_primary_shop_id = ssd.shop_id 
    AND date_diff(DATE(CURRENT_DATE()),DATE(ssd.datetime),day)<= 365

WHERE TRUE
  AND (COALESCE(f.activation_count,0) > 0 OR COALESCE(f.plus_upgrade_count,0) > 0)
  AND md.currently_associated_referral_type IN ("Development Shop Referral")
  AND md.merchant_type = "Standard"
  AND cmd.commercial_region_code IN ("APAC")
  AND pd.current_commercial_region_code IN ("APAC")
  AND date(f.transition_at) >= date(2018,1,1)
  AND md.merchant_country_name = "Japan"
  AND pd.partner_country = "JP"
  AND md.current_merchant_active_status = "Active"
  AND date_diff(DATE(current_date()),DATE(f.transition_at), day) >= 180  
  
GROUP BY 
  1,2,3,4,5,6,7,8,9
  
HAVING
  sum(daily_gmv_usd) >= 50000
  
ORDER BY 
  9 desc
) t1 
left join `shopify-data-bigquery-global.seamster_backroom_revenue.plus_upgrades_outbound_list` t2
on t1.merchant_id = t2.merchant_id
