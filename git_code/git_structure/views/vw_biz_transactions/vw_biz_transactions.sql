/*--...
------------------------------Change Log--------------------------------
2022-06-02 - added fee amount tousd
2022-01-31 - added funded amount tousd
2021-12-17 - added migration partner to identify BV migrated clients
2021-10-12 - changed dim invoices join from account id to client id. also changed fact transactions account id to client id
2021-10-06 - removed dim account dependencies. Changed account sys id logic to look at fundthrough company id and phoenix user id from dim clients. Changed current credit limit and status to phoenix credit limit and phoenix status
2021-09-13 - changed account_platform logic to look at invoice_source_systems
------------------------------------------------------------------------
*/
with  --
first_advance as (
  select
    client_id,
    min(transaction_date) as transaction_date
  from redshift.business.fact_transactions
  where transaction_type = 'A'
        and transaction_status not in ('queued','error','cancelled')
  group by client_id
  ),

source_systems as(
  select distinct
  client_id
  , listagg(distinct source_system, ',') as invoice_source_systems
  from redshift.business.fact_invoices_summary
  group by 1
  ),

first_advance_invoices as (
  select DISTINCT
    invoice_id
  from redshift.business.fact_transactions
    inner join first_advance using (client_id, transaction_date)
  where transaction_type = 'A'
        and transaction_status not in ('queued','error','cancelled')
  )

select
  -- ### TRANSACTION FIELDS ### --
  tx.transaction_id,
  tx.transaction_sys_id,
  tx.transaction_date::date,
  tx.created_at as transaction_created_at,
  tx.updated_at as transaction_updated_at,
  tx.transaction_type,
  tx.transaction_status,
  tx.transaction_description,
  first_advance_invoices.invoice_id is not null as first_funding,
  tx.currency as transaction_currency,
  tx.to_cad_rate,
  tx.source_system as transaction_source_system,
  case tx.source_system 
    when 'jekyll' then 'FTX' 
    when 'phoenix' then 'FTX'
    when 'fundthrough' then 'PRO'
  end as transaction_platform,
  -- ORIGINAL CURRENCY METRICS
  tx.principal_amount as principal_amount_raw,
  tx.interest_amount as interest_amount_raw,
  tx.fee_amount as fee_amount_raw,
  tx.funded_amount as funded_amount_raw,
  tx.revenue_amount as revenue_amount_raw,
  tx.sent_to_client as sent_to_client_raw,
  tx.paid_by_client as paid_by_client_raw,
  tx.paid_by_payor as paid_by_payor_raw,
  -- USD CURRENCY METRICS
  case tx.currency when 'USD' then principal_amount_raw else 0 end as principal_amount_usd,
  case tx.currency when 'USD' then interest_amount_raw else 0 end as interest_amount_usd,
  case tx.currency when 'USD' then fee_amount_raw else 0 end as fee_amount_usd,
  case tx.currency when 'USD' then funded_amount_raw else 0 end as funded_amount_usd,
  case tx.currency when 'USD' then revenue_amount_raw else 0 end as revenue_amount_usd,
  case tx.currency when 'USD' then sent_to_client_raw else 0 end as sent_to_client_usd,
  case tx.currency when 'USD' then paid_by_client_raw else 0 end as paid_by_client_usd,
  case tx.currency when 'USD' then paid_by_payor_raw else 0 end as paid_by_payor_usd,
  -- CAD CURRENCY METRICS
  case tx.currency when 'CAD' then principal_amount_raw else 0 end as principal_amount_cad,
  case tx.currency when 'CAD' then interest_amount_raw else 0 end as interest_amount_cad,
  case tx.currency when 'CAD' then fee_amount_raw else 0 end as fee_amount_cad,
  case tx.currency when 'CAD' then funded_amount_raw else 0 end as funded_amount_cad,
  case tx.currency when 'CAD' then revenue_amount_raw else 0 end as revenue_amount_cad,
  case tx.currency when 'CAD' then sent_to_client_raw else 0 end as sent_to_client_cad,
  case tx.currency when 'CAD' then paid_by_client_raw else 0 end as paid_by_client_cad,
  case tx.currency when 'CAD' then paid_by_payor_raw else 0 end as paid_by_payor_cad,
  -- TO CAD (CAD * 1 and USD * fx) CURRENCY METRICS
  principal_amount_raw * to_cad_rate as principal_amount_tocad,
  interest_amount_raw * to_cad_rate as interest_amount_tocad,
  fee_amount_raw * to_cad_rate as fee_amount_tocad,
  funded_amount_raw * to_cad_rate as funded_amount_tocad,
  revenue_amount_raw * to_cad_rate as revenue_amount_tocad,
  sent_to_client_raw * to_cad_rate as sent_to_client_tocad,
  paid_by_client_raw * to_cad_rate as paid_by_client_tocad,
  paid_by_payor_raw * to_cad_rate as paid_by_payor_tocad,
  (CASE WHEN tx.currency = 'USD' 
          THEN tx.funded_amount
        ELSE tx.funded_amount / tx.usd_to_cad_rate
   END) as funded_amount_tousd,
  (CASE WHEN tx.currency = 'USD' 
          THEN tx.revenue_amount
        ELSE tx.revenue_amount / tx.usd_to_cad_rate
   END) as revenue_amount_tousd,
  (CASE WHEN tx.currency = 'USD'
          THEN tx.fee_amount
        ELSE tx.fee_amount / tx.usd_to_cad_rate
   END) as fee_amount_tousd,
  -- ### ACCOUNT FIELDS ### --
  tx.client_id as account_id,
  (CASE WHEN tx.source_system = 'fundthrough'
          THEN cli.fundthrough_company_id
        ELSE cli.phoenix_user_id
   END) as account_sys_id,
  cli.analytics_id as account_analytics_id,
  cli.ga_client_id as account_ga_client_id,
  cli.full_name as account_full_name,
  cli.first_name as account_first_name,
  cli.last_name as account_last_name,
  cli.email as account_email,
  cli.phone as account_phone,
  cli.country as account_country,
  cli.personal_address_street as personal_address_street,
  cli.personal_address_city as personal_address_city,
  cli.personal_address_state as personal_address_state,
  cli.personal_address_country as personal_address_country,
  cli.personal_address_postal_code as personal_address_postal_code,
  cli.business_address_street as business_address_street,
  cli.business_address_city as business_address_city,
  cli.business_address_state as business_address_state,
  cli.business_address_country as business_address_country,
  cli.business_address_postal_code as business_address_postal_code,
  cli.provider as account_provider,
  cli.marketing_insights as account_marketing_insights,
  cli.phoenix_status as account_status,
  cli.how_you_heard_about_us as account_how_you_heard_about_us,
  cli.is_suspected_fraud as account_is_suspected_fraud,
  cli.is_confirmed_fraud as account_is_confirmed_fraud,
  cli.created_at as account_signup_date,
  cli.score as account_score,
    (CASE WHEN ss.invoice_source_systems ilike '%fundthrough%' and ss.invoice_source_systems ilike '%phoenix%'
            THEN 'PRO,FTX'
          WHEN ss.invoice_source_systems = 'fundthrough'
            THEN 'PRO'
          WHEN ss.invoice_source_systems = 'phoenix'
            THEN 'FTX'
     END) as account_platform,
  -- ### CLIENT FIELDS ### --
  cli.client_id as client_id,
  cli.company_name as client_company_name,
  cli.country as client_country,
  cli.accounting_software_desc as client_accounting_software_desc,
  cli.bank_integration_id as client_bank_integration_id,
  cli.bank_integration_desc as client_bank_integration_desc,
  cli.institution_desc as client_institution_desc,
  cli.industry_code as client_industry_code,
  cli.industry_desc as client_industry_desc,
  cli.adjucation_type as client_adjucation_type,
  cli.years_in_business as client_years_in_business,
  cli.phoenix_credit_limit as client_current_credit_limit,
  cli.partner_source as client_partner,
  cli.created_at as client_signup_date,
  cli.score as client_score,
  -- ### INVOICE FIELDS ### --
  tx.invoice_id,
  inv.invoice_sys_id,
  inv.invoice_number,
  inv.face_amount as invoice_face_amount,
  inv.face_currency as invoice_face_currency,
  inv.invoice_status,
  inv.submitted_date as invoice_submitted_date,
  inv.due_date as invoice_due_date,
  inv.expected_pay_date as invoice_expected_pay_date,
  inv.follow_up_date as invoice_follow_up_date,
  inv.accepted_at as invoice_accepted_at,
  inv.approved_at as invoice_approved_at,
  inv.closed_at as invoice_closed_at,
  inv.advance_rate as invoice_advance_rate,
  inv.funding_fee_rate as invoice_funding_fee_rate,
  inv.transaction_fee_rate as invoice_transaction_fee_rate,
  inv.is_verified as invoice_is_verified,
  inv.has_funds_requested as invoice_has_funds_requested,
  inv.source_system as invoice_source_system, 
  case inv.source_system 
    when 'jekyll' then 'FTX' 
    when 'phoenix' then 'FTX'
    when 'fundthrough' then 'PRO'
  end as invoice_platform,
  -- ### PAYOR FIELDS ### --
  inv.payor_id,
  pay.payor_sys_id,
  pay.company_name as payor_company_name,
  pay.email as payor_email,
  pay.score as payor_score,
  pay.noa_acceptance as payor_noa_policy,
  -- ### PARTNER/BROKER FIELDS (COMMISSION) ### --
  xrf.partner_id as partner_id,
  prt.partner_type as partner_type,
  prt.partner_name as partner_name,
  prt.partner_stage as partner_stage,
  prt.commission_type as partner_commission_type,
  prt.commission_rate as partner_commission_rate,
  prt.sf_owner_id as partner_salesforce_owner_id,
  prt.sf_owner_name as partner_salesforce_owner_name,
  -- ### SOURCE FIELDS ### --
  fas.attribution_date as source_attribution_date,
  fas.category as source_category,
  fas.subcategory as source_subcategory,
  fas.source as account_source,
  fas.matching_pattern as source_matching_pattern,
  fas.all_sources_concat as account_all_sources,
  tx.parent_transaction_sys_id,
  tx.client_cash_event_id, 
  tx.payment_cash_event_id, 
  tx.cash_event_id, 
  tx.cash_event_date,
  cli.migration_partner
from 
  redshift.business.fact_transactions as tx
  left join redshift.business.dim_clients as cli using (client_id)
  left join redshift.business.dim_payors as pay using (payor_id)
  left join redshift.business.dim_invoices as inv using (invoice_id)
  left join redshift.business.xref_accounts_partners as xrf
    on tx.client_id = xrf.client_id
  left join redshift.business.dim_partners as prt using (partner_id)
  left join first_advance_invoices using (invoice_id)
  left join [vw_biz_visitor_sources as fas] on fas.visitor_id = cli.client_id
  left join source_systems as ss 
    on tx.client_id = ss.client_id
where 
  not ( cli.is_dummy or cli.is_admin )
  and case when transaction_type = 'RP' 
             then transaction_status in ('na','submitted','written_off','bankrupt')
           else transaction_status not in ('queued', 'cancelled', 'error')
           end
order by 
  transaction_date, 
  transaction_created_at