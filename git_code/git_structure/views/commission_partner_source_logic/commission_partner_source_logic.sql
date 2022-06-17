WITH sources as(
  SELECT DISTINCT
  fas.client_id
  , first_value(fas.web_referrer) OVER (PARTITION BY fas.client_id ORDER BY fas.attribution_date ASC rows unbounded preceding) as first_referrer
  FROM redshift.business.fact_acquisition_sources fas
--   WHERE fas.client_id IS NOT null
  )

SELECT 
[client_credit_signals_live_test].client_id
, [client_credit_signals_live_test].company_name
, (CASE WHEN [client_credit_signals_live_test].accounting_software_desc ilike '%openinvoice%' OR [client_credit_signals_live_test].accounting_software_desc ilike '%cortex%'
          THEN 'Enverus'
        WHEN [client_credit_signals_live_test].accounting_software_desc ilike '%quickbooks%'
            AND [client_credit_signals_live_test].created_at > '2020-09-07' 
            AND [client_credit_signals_live_test].country = 'CA' 
            AND [client_credit_signals_live_test].client_id NOT IN ('f22b2b90-da51-49df-b37b-2d0a59a0063c', 'f09d9de3-9c88-4c54-83f6-0867c17e6bc3') -- Pinnacle and staffy
          THEN (CASE WHEN [client_credit_signals_live_test].first_referrer ilike '%cortex%' OR [client_credit_signals_live_test].first_referrer ilike '%openinvoice%'
                       THEN 'Enverus'
                     WHEN [client_credit_signals_live_test].client_id IN ('f22b2b90-da51-49df-b37b-2d0a59a0063c', 'f09d9de3-9c88-4c54-83f6-0867c17e6bc3') -- Pinnacle and staffy
                       THEN 'Other'
                     ELSE 'Intuit'
                END) 
        ELSE 'Other'
   END) as commission_partner_source 
, fcs.referring_partner
FROM redshift.business.dim_clients [client_credit_signals_live_test]
  LEFT JOIN sources [client_credit_signals_live_test] USING (client_id)
  LEFT JOIN redshift.business.fact_clients_summary fcs USING (client_id)
WHERE [client_credit_signals_live_test].company_name IS NOT null
      AND NOT [client_credit_signals_live_test].is_dummy
      AND NOT [client_credit_signals_live_test].is_admin
--       AND (commision_partner_source = 'Intuit' or referring_partner = 'intuit')
      AND [client_credit_signals_live_test].email not ilike '%test%'