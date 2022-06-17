with signal_states as (
  SELECT
account_id
, signal_name
, signal_state
, signal_state_desc
, signal_notes
FROM [client_credit_signals_live_test]
WHERE signal_name = 'financials'
AND signal_state != 1 
  )

, company_info as (
  select 
  client_id
, client_company_name
    , client_accounting_software_desc
  , sum(principal_amount_raw) as outstanding_amount

  from [vw_biz_transactions]
  group by 1,2,3
  )

SELECT 
  ci.client_company_name as client
, ci.outstanding_amount as outstanding
, ci.client_accounting_software_desc as accounting
, ss.signal_name as signal_type
-- , ss.signal_state
, ss.signal_state_desc as signal_value
from signal_states as ss
left join company_info as ci on (ss.account_id = ci.client_id)
WHERE (accounting ilike '%OpenInvoice%' and ci.outstanding_amount > 249999)
  OR (accounting ilike '%Cortex%' and ci.outstanding_amount > 249999)
  OR ((ci.outstanding_amount > 99999) and (accounting not ilike '%Cortex%') and (accounting not ilike '%OpenInvoice%'))
--   OR (ci.outstanding_amount > 99999 and accounting not ilike '%OpenInvoice%')
ORDER BY 2 desc