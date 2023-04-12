
  SELECT
    *
  FROM
    (
      SELECT
        date(a.created_date) AS created_date,
        a.id AS campaign_id,
        a.name --,a1.first_name
        --,a1.country
,
        count(DISTINCT lead_or_contact_id) AS members --not sure if this is the correct way to find MQL but it aligns in this one case with what we see in SF
,
        count(
          DISTINCT CASE
            WHEN c.status = 'Qualified' THEN c.company
          END
        ) AS MQL,
        count(
          DISTINCT CASE
            WHEN c.status = 'Qualified'
            AND e.lead_id IS NULL THEN c.company
          END
        ) AS net_new_mql
      FROM
        hive.raw_salesforce_trident.campaigns a --this is just a hacky way to specify APAC campaigns
        JOIN (
          SELECT
            DISTINCT ad_campaign_name,
            region,
            MEDIUM
          FROM
            (
              -- Purpose: This definition provides a common backend for 2020 Plus Funnel reports
              -- Grain: Same as `plus_funnel_daily_rollup`
              -- Context: This query now runs with the `plus_funnel_daily_rollup` as the primary backend.
              --          This query adds a few things to the rollup that are used by old reports, because re-creating
              --          the original query schema lets us avoid re-configuring many visualizations in existing reports.
              --          This query extends the `plus_funnel_daily_rollup` in the following way:
              --              * Adding MCV (an actual metric we use) and New Deals (a legacy metric that should be removed in
              --                reporting but is kept in the query to avoid breaking changes) to the rollup output.
              --              * Adding derived columns used for filter-friendly conversion rate displays (these reports
              --                pre-dated derived columns in Mode).
              --              * Adding extra date columns to limit data for period over period comparisons.
              --              * Changing column names to match the existing report configuration.
              WITH daily_aggregated_mcv AS (
                -- Purpose: Supplements `plus_funnel_daily_rollup` with MCV-based metrics.
                -- Notes: MCV is attributed to marketing activities using the same logic as
                --        Closed Won opportunities, but instead of splitting "1" opportunity
                --        among the touchpoints we split the MCV amount.
                SELECT
                  DATE_TRUNC('day', pod.opportunity_closed_date) AS event_date,
                  pod.cost_center AS cost_center,
                  pod.cost_center_track AS cost_center_track,
                  pod.market_segment AS segment,
                  pod.inferred_region AS region,
                  pod.inferred_country_code AS country_code,
                  pod.deal_type_category AS deal_type,
                  pod.sales_region AS sales_region,
                  pod.upgrade_type AS upgrade_type,
                  pod.attributed_source AS source,
                  attribution_dim.ad_campaign_name AS ad_campaign_name,
                  attribution_dim.ad_content AS ad_content,
                  attribution_dim.keyword_text AS keyword_text,
                  LOWER(attribution_dim.medium) AS MEDIUM,
                  attribution_dim.touchpoint_source AS touchpoint_source,
                  channel_dim.marketing_channel AS marketing_channel,
                  channel_dim.adjusted_marketing_channel_path AS adjusted_marketing_channel_path,
                  channel_dim.marketing_channel_path AS marketing_channel_path,
                  -- These are duplicated to allow for renaming
                  pod.cost_center AS lead_acquisition_path,
                  pod.cost_center_track AS lead_acquisition_sub_path,
                  -- Included for unioning
                  CAST(0 AS DECIMAL(38, 7)) AS net_new_sessions,
                  CAST(0 AS DECIMAL(38, 7)) AS net_new_leads,
                  CAST(0 AS DECIMAL(38, 7)) AS net_new_qualified_leads,
                  CAST(0 AS DECIMAL(38, 7)) AS net_new_prequalified,
                  CAST(0 AS DECIMAL(38, 7)) AS net_new_interested,
                  CAST(0 AS DECIMAL(38, 7)) AS net_new_closed_won,
                  CAST(0 AS DECIMAL(38, 7)) AS net_new_closed_lost,
                  CAST(0 AS DECIMAL(38, 7)) AS net_new_created_opportunities,
                  -- Split MCV amount by % touchpoint attribution
                  SUM(
                    IF(
                      pod.is_won,
                      CAST(pod.monthly_contract_value AS DECIMAL(38, 7)) * attribution_facts.count_w_shaped,
                      CAST(0 AS DECIMAL(38, 7))
                    )
                  ) AS net_new_monthly_contract_value,
                  SUM(
                    IF(
                      pod.is_won,
                      CAST(pod.monthly_contract_value AS DECIMAL(38, 7)) * attribution_facts.count_w_shaped / CAST(2000.0 AS DECIMAL(38, 7)),
                      CAST(0 AS DECIMAL(38, 7))
                    )
                  ) AS net_new_deals_won
                FROM
                  hive.plus.plus_opportunity_dimension AS pod
                  JOIN hive.plus.plus_opportunity_marketing_attribution_facts AS attribution_facts USING (_plus_opportunity_key)
                  JOIN hive.revenue.revenue_touchpoint_attribution_dimension AS attribution_dim USING (_revenue_touchpoint_attribution_key)
                  JOIN hive.plus.plus_marketing_channel_dimension AS channel_dim USING (_plus_marketing_channel_key)
                WHERE
                  pod.is_closed -- Beginning of the Plus funnel rollup; Plus migrates to Salesforce
                  AND pod.opportunity_closed_date >= TIMESTAMP '2019-02-01'
                GROUP BY
                  DATE_TRUNC('day', pod.opportunity_closed_date),
                  pod.cost_center,
                  pod.cost_center_track,
                  pod.market_segment,
                  pod.inferred_region,
                  pod.inferred_country_code,
                  pod.deal_type_category,
                  pod.sales_region,
                  pod.upgrade_type,
                  pod.attributed_source,
                  attribution_dim.ad_campaign_name,
                  attribution_dim.ad_content,
                  attribution_dim.keyword_text,
                  LOWER(attribution_dim.medium),
                  attribution_dim.touchpoint_source,
                  channel_dim.marketing_channel,
                  channel_dim.adjusted_marketing_channel_path,
                  channel_dim.marketing_channel_path,
                  -- These are duplicated to allow for renaming
                  pod.cost_center,
                  pod.cost_center_track
              ),
              funnel_rollup AS (
                -- Purpose: Add reporting dimensions and supplemental metrics to rollup base.
                --          Metrics are re-aggregated to ensure the grain is unique since
                --          not all columns are used from the source rollup.
                SELECT
                  -- Date grain
                  date AS event_date,
                  -- Reporting dimensions
                  source_dim.lead_acquisition_path AS cost_center,
                  source_dim.lead_acquisition_sub_path AS cost_center_track,
                  metric_rollup.segment_for_quota AS segment,
                  metric_rollup.merchant_region AS region,
                  metric_rollup.country_code AS country_code,
                  metric_rollup.deal_type AS deal_type,
                  metric_rollup.sales_region AS sales_region,
                  metric_rollup.upgrade_type AS upgrade_type,
                  source_dim.cleaned_source AS source,
                  attribution_dim.ad_campaign_name AS ad_campaign_name,
                  attribution_dim.ad_content AS ad_content,
                  attribution_dim.keyword_text AS keyword_text,
                  LOWER(attribution_dim.medium) AS MEDIUM,
                  attribution_dim.touchpoint_source AS touchpoint_source,
                  channel_dim.marketing_channel AS marketing_channel,
                  channel_dim.adjusted_marketing_channel_path AS adjusted_marketing_channel_path,
                  channel_dim.marketing_channel_path AS marketing_channel_path,
                  -- Duplicate dimensions included to allow naming updates without breaking change
                  source_dim.lead_acquisition_path AS lead_acquisition_path,
                  source_dim.lead_acquisition_sub_path AS lead_acquisition_sub_path,
                  -- Metrics modelled in rollup
                  SUM(metric_rollup.total_sessions) AS net_new_sessions,
                  SUM(metric_rollup.net_new_leads) AS net_new_leads,
                  SUM(metric_rollup.qualified_leads) AS net_new_qualified_leads,
                  SUM(
                    metric_rollup.prequalified_opportunity_transitions
                  ) AS net_new_prequalified,
                  SUM(
                    metric_rollup.total_interested_opportunity_transitions
                  ) AS net_new_interested,
                  SUM(metric_rollup.total_closed_won_opportunities) AS net_new_closed_won,
                  SUM(metric_rollup.total_closed_lost_opportunities) AS net_new_closed_lost,
                  SUM(metric_rollup.created_opportunities) AS net_new_created_opportunities,
                  -- Metrics not modelled in rollup
                  SUM(CAST(0 AS DECIMAL(38, 7))) AS net_new_monthly_contract_value,
                  SUM(CAST(0 AS DECIMAL(38, 7))) AS net_new_deals_won
                FROM
                  hive.plus.plus_funnel_daily_rollup AS metric_rollup -- Join daily rollup to reporting dimension models
                  JOIN hive.revenue.source_dimension AS source_dim USING (_source_key)
                  JOIN hive.revenue.revenue_touchpoint_attribution_dimension AS attribution_dim USING (_revenue_touchpoint_attribution_key)
                  JOIN hive.plus.plus_marketing_channel_dimension AS channel_dim USING (_plus_marketing_channel_key)
                GROUP BY
                  metric_rollup.date,
                  source_dim.lead_acquisition_path,
                  source_dim.lead_acquisition_sub_path,
                  metric_rollup.segment_for_quota,
                  metric_rollup.merchant_region,
                  metric_rollup.country_code,
                  metric_rollup.deal_type,
                  metric_rollup.sales_region,
                  metric_rollup.upgrade_type,
                  source_dim.cleaned_source,
                  attribution_dim.ad_campaign_name,
                  attribution_dim.ad_content,
                  attribution_dim.keyword_text,
                  LOWER(attribution_dim.medium),
                  attribution_dim.touchpoint_source,
                  channel_dim.marketing_channel,
                  channel_dim.adjusted_marketing_channel_path,
                  channel_dim.marketing_channel_path,
                  source_dim.lead_acquisition_path,
                  source_dim.lead_acquisition_sub_path
              ),
              union_funnel_rollup AS (
                -- Purpose: Combines rows from the Funnel and MCV rollups.
                SELECT
                  *
                FROM
                  funnel_rollup
                UNION
                SELECT
                  *
                FROM
                  daily_aggregated_mcv
              ),
              add_full_country_name AS (
                SELECT
                  *,
                  COALESCE(cd.country_name, 'Unknown country_name') AS country
                FROM
                  union_funnel_rollup AS ufr
                  LEFT JOIN hive.international.country_dimension AS cd USING (country_code)
              ),
              re_aggregate_rollup_to_conform_grain AS (
                -- Purpose: Re-aggregates queries to avoid duplicate rows.
                SELECT
                  event_date,
                  cost_center,
                  cost_center_track,
                  segment,
                  region,
                  country_code,
                  country,
                  deal_type,
                  sales_region,
                  upgrade_type,
                  source,
                  ad_campaign_name,
                  ad_content,
                  keyword_text,
                  MEDIUM,
                  touchpoint_source,
                  marketing_channel,
                  adjusted_marketing_channel_path,
                  marketing_channel_path,
                  lead_acquisition_path,
                  lead_acquisition_sub_path,
                  SUM(net_new_sessions) AS net_new_sessions,
                  SUM(net_new_leads) AS net_new_leads,
                  SUM(net_new_qualified_leads) AS net_new_qualified_leads,
                  SUM(net_new_prequalified) AS net_new_prequalified,
                  SUM(net_new_interested) AS net_new_interested,
                  SUM(net_new_closed_won) AS net_new_closed_won,
                  SUM(net_new_closed_lost) AS net_new_closed_lost,
                  SUM(net_new_created_opportunities) AS net_new_created_opportunities,
                  SUM(net_new_monthly_contract_value) AS net_new_monthly_contract_value,
                  SUM(net_new_deals_won) AS net_new_deals_won
                FROM
                  add_full_country_name
                GROUP BY
                  event_date,
                  cost_center,
                  cost_center_track,
                  segment,
                  region,
                  country_code,
                  country,
                  deal_type,
                  sales_region,
                  upgrade_type,
                  source,
                  ad_campaign_name,
                  ad_content,
                  keyword_text,
                  MEDIUM,
                  touchpoint_source,
                  marketing_channel,
                  adjusted_marketing_channel_path,
                  marketing_channel_path,
                  lead_acquisition_path,
                  lead_acquisition_sub_path
              ),
              combined_funnel_rollup_with_conversion_columns AS (
                -- Purpose: Adds derived columns that are used for calculating conversion rates from additive facts.
                -- Notes: Because of the sparsity of this dataset, these columns might not make sense on a row-by-row
                --        basis or for small categories; this is intended for aggregated results.
                SELECT
                  reaggregated.*,
                  reaggregated.net_new_sessions - reaggregated.net_new_leads AS _net_new_sessions_not_leads,
                  reaggregated.net_new_leads - reaggregated.net_new_qualified_leads AS _net_new_leads_not_qualified,
                  reaggregated.net_new_qualified_leads - net_new_prequalified AS _net_new_qualified_leads_not_prequalified,
                  reaggregated.net_new_qualified_leads - net_new_interested AS _net_new_qualified_leads_not_interested,
                  reaggregated.net_new_qualified_leads - net_new_closed_won AS _net_new_qualified_leads_not_closed_won,
                  reaggregated.net_new_prequalified - net_new_interested AS _net_new_prequalified_not_interested,
                  reaggregated.net_new_prequalified - net_new_closed_won AS _net_new_prequalified_not_closed_won,
                  reaggregated.net_new_created_opportunities - net_new_interested AS _net_new_created_opportunities_not_interested,
                  reaggregated.net_new_created_opportunities - net_new_closed_won AS _net_new_created_opportunities_not_closed_won,
                  reaggregated.net_new_interested - net_new_closed_won AS _net_new_interested_not_closed_won
                FROM
                  re_aggregate_rollup_to_conform_grain AS reaggregated
              ),
              add_date_attributes AS (
                -- Purpose: Adds derived date attributes that are used for chart configurations.
                SELECT
                  *,
                  (event_date - DATE_TRUNC('month', event_date)) < CURRENT_TIMESTAMP - DATE_TRUNC('month', CURRENT_TIMESTAMP) AS is_in_month_to_date_range,
                  (event_date - DATE_TRUNC('quarter', event_date)) < CURRENT_TIMESTAMP - DATE_TRUNC('quarter', CURRENT_TIMESTAMP) AS is_in_quarter_to_date_range,
                  (event_date - DATE_TRUNC('year', event_date)) < CURRENT_TIMESTAMP - DATE_TRUNC('year', CURRENT_TIMESTAMP) AS is_in_year_to_date_range,
                  DATE_TRUNC('month', event_date) = DATE_TRUNC('month', CURRENT_TIMESTAMP) AS is_current_month,
                  DATE_TRUNC('quarter', event_date) = DATE_TRUNC('quarter', CURRENT_TIMESTAMP) AS is_current_quarter,
                  DATE_TRUNC('year', event_date) = DATE_TRUNC('year', CURRENT_TIMESTAMP) AS is_current_year
                FROM
                  combined_funnel_rollup_with_conversion_columns
              )
              SELECT
                *
              FROM
                add_date_attributes
              WHERE
                -- Limit output until after Plus top of funnel
                -- stabilized in Salesforce.
                event_date >= TIMESTAMP '2019-07-01'
            ) AS kpi
          WHERE
            kpi.sales_region = 'APAC'
        ) kpi ON a.name = kpi.ad_campaign_name
        JOIN hive.raw_salesforce_trident.campaign_members b ON a.id = b.campaign_id
        LEFT JOIN hive.raw_salesforce_trident.leads c ON b.lead_id = c.id
        LEFT JOIN hive.raw_salesforce_trident.leads_history e ON c.id = e.lead_id
        AND e.created_at < a.created_date
      WHERE
        a.created_date >= date('2022-01-01')
        AND (
          c.domain_name NOT LIKE '%shopify.com'
          OR c.domain_name IS NULL
        ) --and (region_detail = 'APAC' or a.created_by_id in ('0053u000004MHJ6AAO','0056A0000024WA2QAM'))
      GROUP BY
        1,
        2,
        3
    ) t1
    LEFT JOIN (
      SELECT
        campaign_id,
        SUM(
          CASE
            WHEN stage_name = 'Closed Won' THEN total_amount
            ELSE 0
          END
        ) AS closed_won_amount,
        SUM(
          CASE
            WHEN stage_name = 'Closed Lost' THEN total_amount
            ELSE 0
          END
        ) AS closed_lost_amount,
        SUM(
          CASE
            WHEN stage_name IN (
              'Solution Validation',
              'Exploration',
              'Contracting',
              'Term-Sheet',
              'On Hold',
              'Negotiation',
              'Open',
              'Prospect',
              'Discovery',
              'New',
              'Pre-Qualified',
              'Committed',
              'Qualified',
              'Booked',
              'Fit Confirmed',
              'In Progress',
              'Proposal',
              'Contract Sent',
              'Interest',
              'SAL',
              'Evaluation',
              'Demo',
              'Scoping'
            ) THEN total_amount
            ELSE 0
          END
        ) AS in_progress_amount,
        SUM(
          CASE
            WHEN stage_name = 'Closed Won' THEN number_of_opportunities
            ELSE 0
          END
        ) AS closed_won_optys,
        SUM(
          CASE
            WHEN stage_name = 'Closed Lost' THEN number_of_opportunities
            ELSE 0
          END
        ) AS closed_lost_optys,
        SUM(
          CASE
            WHEN stage_name IN (
              'Solution Validation',
              'Exploration',
              'Contracting',
              'Term-Sheet',
              'On Hold',
              'Negotiation',
              'Open',
              'Prospect',
              'Discovery',
              'New',
              'Pre-Qualified',
              'Committed',
              'Qualified',
              'Booked',
              'Fit Confirmed',
              'In Progress',
              'Proposal',
              'Contract Sent',
              'Interest',
              'SAL',
              'Evaluation',
              'Demo',
              'Scoping'
            ) THEN number_of_opportunities
            ELSE 0
          END
        ) AS in_progress_optys
      FROM
        (
          SELECT
            campaign_id,
            stage_name,
            sum(amount) AS total_amount,
            count(*) AS number_of_opportunities
          FROM
            hive.raw_salesforce_trident.opportunities
          WHERE
            campaign_id IN (
              SELECT
                campaign_id
              FROM
                (
                  SELECT
                    date(a.created_date) AS created_date,
                    a.id AS campaign_id,
                    a.name --,a1.first_name
                    --,a1.country
,
                    count(DISTINCT lead_or_contact_id) AS members --not sure if this is the correct way to find MQL but it aligns in this one case with what we see in SF
,
                    count(
                      DISTINCT CASE
                        WHEN c.status = 'Qualified' THEN c.company
                      END
                    ) AS MQL,
                    count(
                      DISTINCT CASE
                        WHEN c.status = 'Qualified'
                        AND e.lead_id IS NULL THEN c.company
                      END
                    ) AS net_new_mql
                  FROM
                    hive.raw_salesforce_trident.campaigns a --this is just a hacky way to specify APAC campaigns
                    JOIN (
                      SELECT
                        DISTINCT ad_campaign_name,
                        region,
                        MEDIUM
                      FROM
                        (
                          -- Purpose: This definition provides a common backend for 2020 Plus Funnel reports
                          -- Grain: Same as `plus_funnel_daily_rollup`
                          -- Context: This query now runs with the `plus_funnel_daily_rollup` as the primary backend.
                          --          This query adds a few things to the rollup that are used by old reports, because re-creating
                          --          the original query schema lets us avoid re-configuring many visualizations in existing reports.
                          --          This query extends the `plus_funnel_daily_rollup` in the following way:
                          --              * Adding MCV (an actual metric we use) and New Deals (a legacy metric that should be removed in
                          --                reporting but is kept in the query to avoid breaking changes) to the rollup output.
                          --              * Adding derived columns used for filter-friendly conversion rate displays (these reports
                          --                pre-dated derived columns in Mode).
                          --              * Adding extra date columns to limit data for period over period comparisons.
                          --              * Changing column names to match the existing report configuration.
                          WITH daily_aggregated_mcv AS (
                            -- Purpose: Supplements `plus_funnel_daily_rollup` with MCV-based metrics.
                            -- Notes: MCV is attributed to marketing activities using the same logic as
                            --        Closed Won opportunities, but instead of splitting "1" opportunity
                            --        among the touchpoints we split the MCV amount.
                            SELECT
                              DATE_TRUNC('day', pod.opportunity_closed_date) AS event_date,
                              pod.cost_center AS cost_center,
                              pod.cost_center_track AS cost_center_track,
                              pod.market_segment AS segment,
                              pod.inferred_region AS region,
                              pod.inferred_country_code AS country_code,
                              pod.deal_type_category AS deal_type,
                              pod.sales_region AS sales_region,
                              pod.upgrade_type AS upgrade_type,
                              pod.attributed_source AS source,
                              attribution_dim.ad_campaign_name AS ad_campaign_name,
                              attribution_dim.ad_content AS ad_content,
                              attribution_dim.keyword_text AS keyword_text,
                              LOWER(attribution_dim.medium) AS MEDIUM,
                              attribution_dim.touchpoint_source AS touchpoint_source,
                              channel_dim.marketing_channel AS marketing_channel,
                              channel_dim.adjusted_marketing_channel_path AS adjusted_marketing_channel_path,
                              channel_dim.marketing_channel_path AS marketing_channel_path,
                              -- These are duplicated to allow for renaming
                              pod.cost_center AS lead_acquisition_path,
                              pod.cost_center_track AS lead_acquisition_sub_path,
                              -- Included for unioning
                              CAST(0 AS DECIMAL(38, 7)) AS net_new_sessions,
                              CAST(0 AS DECIMAL(38, 7)) AS net_new_leads,
                              CAST(0 AS DECIMAL(38, 7)) AS net_new_qualified_leads,
                              CAST(0 AS DECIMAL(38, 7)) AS net_new_prequalified,
                              CAST(0 AS DECIMAL(38, 7)) AS net_new_interested,
                              CAST(0 AS DECIMAL(38, 7)) AS net_new_closed_won,
                              CAST(0 AS DECIMAL(38, 7)) AS net_new_closed_lost,
                              CAST(0 AS DECIMAL(38, 7)) AS net_new_created_opportunities,
                              -- Split MCV amount by % touchpoint attribution
                              SUM(
                                IF(
                                  pod.is_won,
                                  CAST(pod.monthly_contract_value AS DECIMAL(38, 7)) * attribution_facts.count_w_shaped,
                                  CAST(0 AS DECIMAL(38, 7))
                                )
                              ) AS net_new_monthly_contract_value,
                              SUM(
                                IF(
                                  pod.is_won,
                                  CAST(pod.monthly_contract_value AS DECIMAL(38, 7)) * attribution_facts.count_w_shaped / CAST(2000.0 AS DECIMAL(38, 7)),
                                  CAST(0 AS DECIMAL(38, 7))
                                )
                              ) AS net_new_deals_won
                            FROM
                              hive.plus.plus_opportunity_dimension AS pod
                              JOIN hive.plus.plus_opportunity_marketing_attribution_facts AS attribution_facts USING (_plus_opportunity_key)
                              JOIN hive.revenue.revenue_touchpoint_attribution_dimension AS attribution_dim USING (_revenue_touchpoint_attribution_key)
                              JOIN hive.plus.plus_marketing_channel_dimension AS channel_dim USING (_plus_marketing_channel_key)
                            WHERE
                              pod.is_closed -- Beginning of the Plus funnel rollup; Plus migrates to Salesforce
                              AND pod.opportunity_closed_date >= TIMESTAMP '2019-02-01'
                            GROUP BY
                              DATE_TRUNC('day', pod.opportunity_closed_date),
                              pod.cost_center,
                              pod.cost_center_track,
                              pod.market_segment,
                              pod.inferred_region,
                              pod.inferred_country_code,
                              pod.deal_type_category,
                              pod.sales_region,
                              pod.upgrade_type,
                              pod.attributed_source,
                              attribution_dim.ad_campaign_name,
                              attribution_dim.ad_content,
                              attribution_dim.keyword_text,
                              LOWER(attribution_dim.medium),
                              attribution_dim.touchpoint_source,
                              channel_dim.marketing_channel,
                              channel_dim.adjusted_marketing_channel_path,
                              channel_dim.marketing_channel_path,
                              -- These are duplicated to allow for renaming
                              pod.cost_center,
                              pod.cost_center_track
                          ),
                          funnel_rollup AS (
                            -- Purpose: Add reporting dimensions and supplemental metrics to rollup base.
                            --          Metrics are re-aggregated to ensure the grain is unique since
                            --          not all columns are used from the source rollup.
                            SELECT
                              -- Date grain
                              date AS event_date,
                              -- Reporting dimensions
                              source_dim.lead_acquisition_path AS cost_center,
                              source_dim.lead_acquisition_sub_path AS cost_center_track,
                              metric_rollup.segment_for_quota AS segment,
                              metric_rollup.merchant_region AS region,
                              metric_rollup.country_code AS country_code,
                              metric_rollup.deal_type AS deal_type,
                              metric_rollup.sales_region AS sales_region,
                              metric_rollup.upgrade_type AS upgrade_type,
                              source_dim.cleaned_source AS source,
                              attribution_dim.ad_campaign_name AS ad_campaign_name,
                              attribution_dim.ad_content AS ad_content,
                              attribution_dim.keyword_text AS keyword_text,
                              LOWER(attribution_dim.medium) AS MEDIUM,
                              attribution_dim.touchpoint_source AS touchpoint_source,
                              channel_dim.marketing_channel AS marketing_channel,
                              channel_dim.adjusted_marketing_channel_path AS adjusted_marketing_channel_path,
                              channel_dim.marketing_channel_path AS marketing_channel_path,
                              -- Duplicate dimensions included to allow naming updates without breaking change
                              source_dim.lead_acquisition_path AS lead_acquisition_path,
                              source_dim.lead_acquisition_sub_path AS lead_acquisition_sub_path,
                              -- Metrics modelled in rollup
                              SUM(metric_rollup.total_sessions) AS net_new_sessions,
                              SUM(metric_rollup.net_new_leads) AS net_new_leads,
                              SUM(metric_rollup.qualified_leads) AS net_new_qualified_leads,
                              SUM(
                                metric_rollup.prequalified_opportunity_transitions
                              ) AS net_new_prequalified,
                              SUM(
                                metric_rollup.total_interested_opportunity_transitions
                              ) AS net_new_interested,
                              SUM(metric_rollup.total_closed_won_opportunities) AS net_new_closed_won,
                              SUM(metric_rollup.total_closed_lost_opportunities) AS net_new_closed_lost,
                              SUM(metric_rollup.created_opportunities) AS net_new_created_opportunities,
                              -- Metrics not modelled in rollup
                              SUM(CAST(0 AS DECIMAL(38, 7))) AS net_new_monthly_contract_value,
                              SUM(CAST(0 AS DECIMAL(38, 7))) AS net_new_deals_won
                            FROM
                              hive.plus.plus_funnel_daily_rollup AS metric_rollup -- Join daily rollup to reporting dimension models
                              JOIN hive.revenue.source_dimension AS source_dim USING (_source_key)
                              JOIN hive.revenue.revenue_touchpoint_attribution_dimension AS attribution_dim USING (_revenue_touchpoint_attribution_key)
                              JOIN hive.plus.plus_marketing_channel_dimension AS channel_dim USING (_plus_marketing_channel_key)
                            GROUP BY
                              metric_rollup.date,
                              source_dim.lead_acquisition_path,
                              source_dim.lead_acquisition_sub_path,
                              metric_rollup.segment_for_quota,
                              metric_rollup.merchant_region,
                              metric_rollup.country_code,
                              metric_rollup.deal_type,
                              metric_rollup.sales_region,
                              metric_rollup.upgrade_type,
                              source_dim.cleaned_source,
                              attribution_dim.ad_campaign_name,
                              attribution_dim.ad_content,
                              attribution_dim.keyword_text,
                              LOWER(attribution_dim.medium),
                              attribution_dim.touchpoint_source,
                              channel_dim.marketing_channel,
                              channel_dim.adjusted_marketing_channel_path,
                              channel_dim.marketing_channel_path,
                              source_dim.lead_acquisition_path,
                              source_dim.lead_acquisition_sub_path
                          ),
                          union_funnel_rollup AS (
                            -- Purpose: Combines rows from the Funnel and MCV rollups.
                            SELECT
                              *
                            FROM
                              funnel_rollup
                            UNION
                            SELECT
                              *
                            FROM
                              daily_aggregated_mcv
                          ),
                          add_full_country_name AS (
                            SELECT
                              *,
                              COALESCE(cd.country_name, 'Unknown country_name') AS country
                            FROM
                              union_funnel_rollup AS ufr
                              LEFT JOIN hive.international.country_dimension AS cd USING (country_code)
                          ),
                          re_aggregate_rollup_to_conform_grain AS (
                            -- Purpose: Re-aggregates queries to avoid duplicate rows.
                            SELECT
                              event_date,
                              cost_center,
                              cost_center_track,
                              segment,
                              region,
                              country_code,
                              country,
                              deal_type,
                              sales_region,
                              upgrade_type,
                              source,
                              ad_campaign_name,
                              ad_content,
                              keyword_text,
                              MEDIUM,
                              touchpoint_source,
                              marketing_channel,
                              adjusted_marketing_channel_path,
                              marketing_channel_path,
                              lead_acquisition_path,
                              lead_acquisition_sub_path,
                              SUM(net_new_sessions) AS net_new_sessions,
                              SUM(net_new_leads) AS net_new_leads,
                              SUM(net_new_qualified_leads) AS net_new_qualified_leads,
                              SUM(net_new_prequalified) AS net_new_prequalified,
                              SUM(net_new_interested) AS net_new_interested,
                              SUM(net_new_closed_won) AS net_new_closed_won,
                              SUM(net_new_closed_lost) AS net_new_closed_lost,
                              SUM(net_new_created_opportunities) AS net_new_created_opportunities,
                              SUM(net_new_monthly_contract_value) AS net_new_monthly_contract_value,
                              SUM(net_new_deals_won) AS net_new_deals_won
                            FROM
                              add_full_country_name
                            GROUP BY
                              event_date,
                              cost_center,
                              cost_center_track,
                              segment,
                              region,
                              country_code,
                              country,
                              deal_type,
                              sales_region,
                              upgrade_type,
                              source,
                              ad_campaign_name,
                              ad_content,
                              keyword_text,
                              MEDIUM,
                              touchpoint_source,
                              marketing_channel,
                              adjusted_marketing_channel_path,
                              marketing_channel_path,
                              lead_acquisition_path,
                              lead_acquisition_sub_path
                          ),
                          combined_funnel_rollup_with_conversion_columns AS (
                            -- Purpose: Adds derived columns that are used for calculating conversion rates from additive facts.
                            -- Notes: Because of the sparsity of this dataset, these columns might not make sense on a row-by-row
                            --        basis or for small categories; this is intended for aggregated results.
                            SELECT
                              reaggregated.*,
                              reaggregated.net_new_sessions - reaggregated.net_new_leads AS _net_new_sessions_not_leads,
                              reaggregated.net_new_leads - reaggregated.net_new_qualified_leads AS _net_new_leads_not_qualified,
                              reaggregated.net_new_qualified_leads - net_new_prequalified AS _net_new_qualified_leads_not_prequalified,
                              reaggregated.net_new_qualified_leads - net_new_interested AS _net_new_qualified_leads_not_interested,
                              reaggregated.net_new_qualified_leads - net_new_closed_won AS _net_new_qualified_leads_not_closed_won,
                              reaggregated.net_new_prequalified - net_new_interested AS _net_new_prequalified_not_interested,
                              reaggregated.net_new_prequalified - net_new_closed_won AS _net_new_prequalified_not_closed_won,
                              reaggregated.net_new_created_opportunities - net_new_interested AS _net_new_created_opportunities_not_interested,
                              reaggregated.net_new_created_opportunities - net_new_closed_won AS _net_new_created_opportunities_not_closed_won,
                              reaggregated.net_new_interested - net_new_closed_won AS _net_new_interested_not_closed_won
                            FROM
                              re_aggregate_rollup_to_conform_grain AS reaggregated
                          ),
                          add_date_attributes AS (
                            -- Purpose: Adds derived date attributes that are used for chart configurations.
                            SELECT
                              *,
                              (event_date - DATE_TRUNC('month', event_date)) < CURRENT_TIMESTAMP - DATE_TRUNC('month', CURRENT_TIMESTAMP) AS is_in_month_to_date_range,
                              (event_date - DATE_TRUNC('quarter', event_date)) < CURRENT_TIMESTAMP - DATE_TRUNC('quarter', CURRENT_TIMESTAMP) AS is_in_quarter_to_date_range,
                              (event_date - DATE_TRUNC('year', event_date)) < CURRENT_TIMESTAMP - DATE_TRUNC('year', CURRENT_TIMESTAMP) AS is_in_year_to_date_range,
                              DATE_TRUNC('month', event_date) = DATE_TRUNC('month', CURRENT_TIMESTAMP) AS is_current_month,
                              DATE_TRUNC('quarter', event_date) = DATE_TRUNC('quarter', CURRENT_TIMESTAMP) AS is_current_quarter,
                              DATE_TRUNC('year', event_date) = DATE_TRUNC('year', CURRENT_TIMESTAMP) AS is_current_year
                            FROM
                              combined_funnel_rollup_with_conversion_columns
                          )
                          SELECT
                            *
                          FROM
                            add_date_attributes
                          WHERE
                            -- Limit output until after Plus top of funnel
                            -- stabilized in Salesforce.
                            event_date >= TIMESTAMP '2019-07-01'
                        ) AS kpi
                      WHERE
                        kpi.sales_region = 'APAC'
                    ) kpi ON a.name = kpi.ad_campaign_name
                    JOIN hive.raw_salesforce_trident.campaign_members b ON a.id = b.campaign_id
                    LEFT JOIN hive.raw_salesforce_trident.leads c ON b.lead_id = c.id
                    LEFT JOIN hive.raw_salesforce_trident.leads_history e ON c.id = e.lead_id
                    AND e.created_at < a.created_date
                  WHERE
                    a.created_date >= date('2022-01-01')
                    AND (
                      c.domain_name NOT LIKE '%shopify.com'
                      OR c.domain_name IS NULL
                    ) --and (region_detail = 'APAC' or a.created_by_id in ('0053u000004MHJ6AAO','0056A0000024WA2QAM'))
                  GROUP BY
                    1,
                    2,
                    3
                ) t1
            )
          GROUP BY
            1,
            2
        ) sub
      GROUP BY
        1
    ) conversion_data ON t1.campaign_id = conversion_data.campaign_id
