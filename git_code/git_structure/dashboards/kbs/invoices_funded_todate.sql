WITH accounting_channels as(
  SELECT
  client_id
  , is_admin
  , is_dummy
  , (CASE WHEN [client_credit_signals_live_test].accounting_software_desc ilike '%quickbooks%'
                 THEN 'Intuit'
               WHEN [client_credit_signals_live_test].accounting_software_desc = 'openinvoice' 
                    OR [client_credit_signals_live_test].accounting_software_desc = 'cortex'
                 THEN 'Enverus'
               ELSE 'Other'
          END) as accounting_channel_
  FROM redshift.business.dim_clients [client_credit_signals_live_test]
  WHERE [[client_credit_signals_live_test].client_id=Company_Names]
        and [[client_credit_signals_live_test].country=country]
        and [[client_credit_signals_live_test].funding_type=Product]
        and [[client_credit_signals_live_test].business_address_state=geography]
        and [[client_credit_signals_live_test].industry_desc=kbs_industry]
        and [accounting_channel_=KBS_Accounting_Channel]
  )

, base as (
  select
  [transaction_date:aggregation] as period,
  client_id,
  invoice_id,
  transaction_platform as serie,
  avg(invoice_face_amount * to_cad_rate) as invoice_size,
  sum(funded_amount_tocad) as amount_funded
  from [vw_biz_transactions]
    inner join accounting_channels using (client_id)
  where transaction_date <= current_date
  and transaction_type = 'A'
  and [client_id=Client]
  and [source_category=Acquisition_Category]
  and [source_subcategory=Acquisition_Subcategory]
  and [account_source=Acquisition_Source]  
  group by 1,2,3,4

  union

  select
  [transaction_date:aggregation] as period,
  client_id,
  invoice_id,
  'All' as serie,
  avg(invoice_face_amount * to_cad_rate) as invoice_size,
  sum(funded_amount_tocad) as amount_funded
  from [vw_biz_transactions]
    inner join accounting_channels using (client_id)
  where transaction_date <= current_date
  and transaction_type = 'A'
  and [client_id=Client]
  and [source_category=Acquisition_Category]
  and [source_subcategory=Acquisition_Subcategory]
  and [account_source=Acquisition_Source]  
  group by 1,2,3,4)

select 
period,
serie,
count(distinct client_id) as clients_funded,
count(distinct invoice_id) as invoices_funded,
[sum(amount_funded):$] as amount_funded,
[sum(amount_funded)/invoices_funded:$] as avg_funded_per_invoice, 
[avg(invoice_size):$] as avg_invoice_size
from base
group by 1,2
order by 1 desc, 2 asc