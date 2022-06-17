/*
----------------------Change Log--------------------------
2021-11-01 - added outstanding amounts
2021-10-06 - changed dim clients status reference to phoenix status
2021-12-08 - Added BlueVine Account Managers Name from static list (need to update to pull from salesforce )
----------------------------------------------------------
*/

with approved_clients as(
  select
    cc.account_id as client_id
    , min( case when signal_state in (1,3) then 1 else 0 end )
        as client_fundable
    , trim( ', ' from listagg( case when signal_state = 3 -- and signal_required
                                    then signal_name end, ', ' ) )
        as client_action_required
    , cc.credit_portfolio_id
    , cc.credit_profile_id
    , cc.account_sys_id
  from [client_credit_signals_live_test as cc]
  where signal_required
  group by 1,4,5,6
  )

, approved_payors as(
  select distinct
    pc.payor_id
  , pc.payor_sys_id
    , p.company_name
    , min( case when signal_state in (1,3) then 1 else 0 end )
      as payor_fundable
    , pc.credit_profile_id
  from [payor_credit_signals as pc]
    left join redshift.business.dim_payors p using (payor_id)
  where signal_required
  group by 1,2,3,5
  )

, disapproved_clients as (
  select cc.account_id as client_id
    , max( case when signal_state NOT IN (1,3) then 1 else 0 end )
        as client_unfundable
    , trim( ', ' from listagg( case when signal_state = 3 -- and signal_required
                                    then signal_name end, ', ' ) )
        as client_action_required
    , cc.credit_portfolio_id
    , cc.credit_profile_id
    , cc.account_sys_id
  from [client_credit_signals_live_test as cc]
  where signal_required
        and recency_rank = 1
        --and client_id = 'b935f3e2af057a081a620849ba74940ffece1531'
  group by 1,4,5,6
  )

, red_flag_signal as (
   select distinct
     cc.account_id  as client_id
    , max(signal_value) as red_flag_signal_value
    , max(signal_notes) as red_flag_signal_notes
  from [client_credit_signals_live_test as cc]
  where signal_required
        and signal_name = 'red_flag'
        and signal_state in (1,3)
  group by 1
  )

, approved_customer as(
  select distinct
    cc.payor_id
    , cc.account_id  as client_id
    , p.company_name
    , min( case when signal_state in (1,3) then 1 else 0 end )
      as customer_fundable
    , cc.credit_profile_id
  from [customer_credit_signals as cc]
    left join redshift.business.dim_payors p using (payor_id)
  where signal_required
  group by 1,2,3,5
  )

, doc_confirmation_approved as (
  select 
   distinct
    cc.payor_id
    , cc.account_id as client_id
    , max( case when signal_state = 1 then 0 else 1 end )
      as customer_unfundable
   from [customer_credit_signals as cc]
   where signal_name = 'doc_confirmation'
   group by 1,2
   )

, invoice_verification_signal as (
   select distinct
    cc.payor_id
    , cc.account_id  as client_id
    , max(signal_value) as signal_value
    , max(signal_notes) as signal_notes
    , p.company_name
    , cc.credit_profile_id
  from [customer_credit_signals as cc]
    left join redshift.business.dim_payors p using (payor_id)
  where signal_required
        and signal_name = 'invoice_verification_method'
        and signal_state in (1,3)
  group by 1,2,5,6
  )


, invoice_data as(
  with pro_invoices as(
    select 
      account_id as client_id 
    , payor_sys_id
      , payor_id_ as payor_id
      , 'PRO' as product
      , face_value as amount
      , created_at as request_ts
      , company_name
      , invoice_id
      , null as invoices
    from [pro_invoices_live]
    where fund_requested = '[client_credit_signals_live_test]'
          and invoice_state = 'submitted'
    )
  
   , pro_invoices_2 as ( 
    select 
      client_id 
     , payor_sys_id
      , payor_id_ as payor_id
      , 'PRO' as product
      , face_value as amount
      , created_at as request_ts
      , company_name
      , invoice_id
      , null as invoices
    from [pro_invoices_live]
    where fund_requested = '[client_credit_signals_live_test]'
          and invoice_state = 'submitted'
    ) -- for dual accounts (sourcesystem = jekyll)
  
  , velocity_invoices as(
    select 
      client_id
    , payor_sys_id
      , payor_id
      , 'Velocity' as product
      , outstanding_amount as amount
      , request_date as request_ts
      , company_name
      , null as invoice_id
      , invoices
    from [velocity_credit_requests]
    )
  
  select * from pro_invoices
    union
  select * from velocity_invoices
    union
  select * from pro_invoices_2
  ) -- invoice_data


, dual_funding_exp as (
  WITH listagg as(
    select distinct
    client_id
    , listagg(distinct source_system, ', ') as systems
    from redshift.business.fact_invoices_summary
    group by 1
    )
  select 
    'Dual Funding' as experiance
    , [client_credit_signals_live_test].client_id 
  from redshift.business.dim_clients [client_credit_signals_live_test] 
    left join listagg [client_credit_signals_live_test] using (client_id)
  where [client_credit_signals_live_test].systems ilike '%phoenix%' and [client_credit_signals_live_test].systems ilike '%fundthrough%'
     and [client_credit_signals_live_test].phoenix_status not in ('dummy')
  )   

, actual_product as (
  select 
    [client_credit_signals_live_test].funding_type,
    [client_credit_signals_live_test].accounting_software_desc,
    [client_credit_signals_live_test].phoenix_status,
    [client_credit_signals_live_test].client_id, 
    [client_credit_signals_live_test].is_dummy, 
    [client_credit_signals_live_test].is_admin
  from redshift.business.dim_clients [client_credit_signals_live_test]
  )

, profile_rates as (
  SELECT 
  p.payor_id
-- , afpc.company_id
, [client_credit_signals_live_test].uuid as client_id
, afpc.advance_rate
, afpc.funding_fee
, afpc.transaction_fee
FROM app-fundthrough.public.customers as afpc
  left join app-fundthrough.public.companies [client_credit_signals_live_test] on [client_credit_signals_live_test].id = afpc.company_id
  left join redshift.business.dim_payors p on p.payor_sys_id = afpc.payor_id
  
  )


, profile_rates_uuid_issue as (
  SELECT 
  p.payor_id
-- , afpc.company_id
, ac.client_id as client_id
, afpc.advance_rate
, afpc.funding_fee
, afpc.transaction_fee
FROM app-fundthrough.public.customers as afpc
  left join app-fundthrough.public.companies [client_credit_signals_live_test] on [client_credit_signals_live_test].id = afpc.company_id
  left join redshift.business.dim_clients ac on ac.client_id = [client_credit_signals_live_test].uuid 
  left join redshift.business.dim_payors p on p.payor_sys_id = afpc.payor_id
  
  )

, duplicate_check as (      
        select 
        'phoenix' as sourcesystem, 
         pi.number as client_invoice_number
        from 
        app-phoenix.public.invoices pi 
        
        union 
        
        select 
        'jekyll' as sourcesystem, 
        ji.number as client_invoice_number
        
        from redshift.jekyll_data.invoices ji 
    
        union
        
        select 
        'fundthrough' as sourcesystem, 
        ft.number as client_invoice_number
        
        from app-fundthrough.public.invoices ft
    )
        
, duplicate_check_final as (
        select 
        client_id, 
        dc.client_invoice_number, 
        dc.sourcesystem,
        count( distinct dc.client_invoice_number ) as inv_number 
        
        from [pro_invoices_live as p] 
        left join duplicate_check dc using (client_invoice_number)
        group by 1,2,3
        having (inv_number > 1) 
     )


  
, base as(
  select 
    acl.client_id
    , max(acl.client_action_required) as client_action_required
    , ap.company_name as payor_company_name
    , acu.company_name as customer_name
    , app.accounting_software_desc as accounting_software_desc
    , ivs.signal_value as verification_type
    , ivs.signal_notes as verification_notes
    , sum(id.amount) as client_total
    , min(request_ts) as request_ts
    , id.company_name as client_company_name
    , acl.credit_portfolio_id
    , ap.credit_profile_id 
    , acu.credit_profile_id
    , acl.credit_profile_id
    , acl.account_sys_id
    , '[ ' || translate( id.company_name, '()"\'', '' )
          || ']'
          ||  '(https://credit.fundthrough.com/admin/credit_portfolios/'
          || acl.credit_portfolio_id
          || ')'
        as client_portfolio_
    , '[ ' || translate( ap.company_name, '()"\'', '' )
          || ']'
          || '(https://credit.fundthrough.com/admin/credit_profiles/'
          || ap.credit_profile_id
          || ')'
        as payor_profile_
       
    , '[ ' || translate( id.company_name || ' | ' || ap.company_name, '()"\'', '' )
          || ']'
          || '(https://credit.fundthrough.com/admin/credit_profiles/'
          || acu.credit_profile_id
          || ')'
        as customer_name_
    , '[ ' || translate( id.company_name, '()"\'', '' )
          || ']'
          || '(https://credit.fundthrough.com/admin/credit_profiles/'
          || acl.credit_profile_id
          || ')'
        as client_profile_
    , count(*) as num_invoices
    , '[' || num_invoices || ']'
          || '(https://app.fundthrough.com/admin/invoices?utf8=%E2%9C%93&q%5Bby_id_in%5D='
          || listagg( id.invoice_id, '%2C' )
          || '&comit=Filter&order=id_desc&per_page='
          || count( id.invoice_id )::varchar
          || ')'
        as pro_invoices_
    , max(id.invoices) as express_invoices_
    , '[' || translate(( datediff( hours, min(request_ts), sysdate ) / 24.0)::decimal(18,1), '()"\'', '' )
         || ']'
         || '(https://app.fundthrough.com/admin/invoices?comit=Filter&order=due_date_asc&per_page=9&q%5Bby_state_in%5D=approved&q%5Bcustomer_company_id_eq%5D='
         || acl.account_sys_id
         || '&utf8=✓)'
        as age
    ,  rf.red_flag_signal_value as red_flag_value
    ,  rf.red_flag_signal_notes as red_flag_notes
    , app.funding_type
, id.payor_sys_id
  ,nvl(pr.advance_rate, pri.advance_rate) as advance_rate
, nvl(pr.funding_fee, pri.funding_fee) as funding_fee
, nvl(pr.transaction_fee, pri.transaction_fee) as transaction_fee
, case when dcf.client_id is not null then 'Check for duplicates' 
        else 'Ok' 
        end as duplicate_invoice_check
  from invoice_data id
    inner join approved_clients acl using (client_id)
    inner join approved_payors ap using (payor_id)
    inner join approved_customer acu using (payor_id, client_id)
    inner join disapproved_clients dc using (client_id)
    left join duplicate_check_final dcf using (client_id) 
    left join dual_funding_exp dexp using (client_id)
    left join actual_product app using (client_id)
    left join invoice_verification_signal ivs using (client_id, payor_id)
    left join  doc_confirmation_approved doc using (client_id, payor_id)
    left join red_flag_signal rf using (client_id)
    left join profile_rates pr using (client_id, payor_id) 
    left join profile_rates_uuid_issue pri using (client_id, payor_id) 
  where acl.client_fundable = 1
        and ap.payor_fundable = 1
        and acu.customer_fundable = 1
        and dc.client_unfundable = 0
        --and app.status in ('active', 'approved')
        and doc.customer_unfundable = 0                                              
  group by 1,3,4,5,6,7,10,11,12,13,14,15,16,24,25,26,27,28,29,30,31
  )

, account_owner as ( 
  SELECT
    xref.ft_client_id as client_id 
  , cd.client_name
  , cd.lead_owner
  FROM [bv_ft_client_xref as xref]
  left join [bv_client_details as cd] on (xref.bv_client_id = cd.client_id)
  )
        
        
-- , main_data as (
select 
  client_total
  , nvl(fab.balance, 0) as outstanding_amount
  , customer_name_
  , cd.lead_owner as account_manager
  , (advance_rate/ 100.0) as advance_rate
  , (funding_fee / 100.0) as funding_fee
  , (transaction_fee/ 100.0) as transaction_fee
  , base.funding_type
  , base.accounting_software_desc
  , verification_type
  , verification_notes
  , age as age_
  , nvl( pro_invoices_, express_invoices_ ) as INV
  , dexp.experiance
  , client_action_required
  , red_flag_value
  , red_flag_notes
  , duplicate_invoice_check
  , client_portfolio_
  , payor_profile_
  , client_profile_
from base 
   left join dual_funding_exp dexp on dexp.client_id = base.client_id
   left join redshift.business.fact_account_balances fab
     on base.client_id = fab.client_id and fab.as_of_date = current_date
   left join app-fundthrough.public.companies [client_credit_signals_live_test]
     on base.client_id = [client_credit_signals_live_test].uuid
   left join account_owner as cd on (base.client_id = cd.client_id)
        
    where (client_action_required is null 
           or  client_action_required = 'red_flag'
           or client_action_required = 'tax_status' 
           )
          AND [client_credit_signals_live_test].migration_partner = 'bluevine'
--           and base.client_id = '875efcf1-5064-4b19-8c47-60353ed1e2d1'
          -- additional filters based on Josie'[client_credit_signals_live_test] request
order by 1 desc