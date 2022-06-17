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
          END) as accounting_channel
  FROM redshift.business.dim_clients [client_credit_signals_live_test]
  WHERE [[client_credit_signals_live_test].client_id=Company_Names]
        and [[client_credit_signals_live_test].country=country]
        and [[client_credit_signals_live_test].funding_type=Product]
        and [[client_credit_signals_live_test].business_address_state=geography]
        and [[client_credit_signals_live_test].industry_desc=kbs_industry]
  )

select 
  [ai.submitted_date:aggregation] as period,
 CASE WHEN [client_credit_signals_live_test].accounting_channel not in ('Enverus', 'Intuit') or [client_credit_signals_live_test].accounting_channel is null
         THEN 'Other'
       ELSE [client_credit_signals_live_test].accounting_channel
  END as segment,
  sum( ai.amount * 
       case 
        when ai.currency = 'USD' then fx.usd_cad 
        else 1
       end
      )::decimal(18,2) as gmv_total,
  [gmv_total:$] as gmv_total_formatted,
  (sum (case
      when fcs.revenue_amount > 0 then 
          case 
            when ai.currency = 'CAD' then ai.amount 
            else ai.amount * fx.usd_cad 
          end
      else 0
    end))::decimal(18,2) as gmv_active,
  [gmv_active:$] as gmv_active_formatted,
  gmv_total - gmv_active  as gmv_remaining_total,
  (sum (case
      when fis.funded_amount > 0 then
          case 
            when ai.currency = 'CAD' then ai.amount 
            else ai.amount * fx.usd_cad 
          end
      else 0
    end))::decimal(18,2) as funded_invoice,
  gmv_active - funded_invoice as gmv_remaining_active,
  [funded_invoice:$] as advanced,
  ((funded_invoice/nullif(gmv_active,0))*100)::decimal(18,2)  as active_client_utilization_pct,
  ((funded_invoice/nullif(gmv_total,0))*100)::decimal(18,2) as total_utilization_pct,
  count(distinct ai.id) as invoice_count
from
   app-braavos.accounting_invoices ai
left join redshift.business.fact_clients_summary fcs on ai.owner_uuid = fcs.client_id
left join business.fact_invoices_summary as fis on ai.owner_uuid= fis.client_id and  ai.client_invoice_number = fis.invoice_number
inner join accounting_channels [client_credit_signals_live_test] on ([client_credit_signals_live_test].client_id = fcs.client_id)
left join redshift.public.exchange_rate fx on (ai.submitted_date = fx.date)
where
  ai.submitted_date <= current_date
  and ai.submitted_date > '2014-07-01'
  and ai.state not in ('draft','deleted')    
  and NVL(ai.doc_type_opt,'') != 'FieldTicket'
  and ai.currency in ('CAD','USD') -- odd accounts in bitcoin
  and owner_uuid not in ('66d5d6f51b1d3fc05b43b961a04cb34a8ac2a5ee', '13397b269c4ff083966a3eaf5fb93c8fb01dddd5','63c894219b574988373b8091f85009ccdf3de5fe', '32a2fb7511dca2336af65c831f677089b69bcf95','01b1f6f01629a5ad8603210accec69ed6866ad85','96dfa57e-72ba-48ca-af91-faf1be8abb0b') -- Odd Billion $ accounts
  and not ( [client_credit_signals_live_test].is_admin or [client_credit_signals_live_test].is_dummy or fcs.is_fraud )
  and [ai.source=AccountingSoftware]
  and [owner_uuid!=Exclude_Clients]
  and [period=daterange_no_tz]
group by
 1,2 

UNION

select 
  [ai.submitted_date:aggregation] as period,
  'Total' as segment, 
  sum( ai.amount * 
       case 
        when ai.currency = 'USD' then fx.usd_cad 
        else 1
       end
      )::decimal(18,2) as gmv_total,
  [gmv_total:$] as gmv_total_formatted,
  (sum (case
      when fcs.revenue_amount > 0 then 
          case 
            when ai.currency = 'CAD' then ai.amount 
            else ai.amount * fx.usd_cad 
          end
      else 0
    end))::decimal(18,2) as gmv_active,
  [gmv_active:$] as gmv_active_formatted,
  gmv_total - gmv_active  as gmv_remaining_total,
  (sum (case
      when fis.funded_amount > 0 then
          case 
            when ai.currency = 'CAD' then ai.amount 
            else ai.amount * fx.usd_cad 
          end
      else 0
    end))::decimal(18,2) as funded_invoice,
  gmv_active - funded_invoice as gmv_remaining_active,
  [funded_invoice:$] as advanced,
  ((funded_invoice/nullif(gmv_active,0))*100)::decimal(18,2)  as active_client_utilization_pct,
  ((funded_invoice/nullif(gmv_total,0))*100)::decimal(18,2) as total_utilization_pct,
  count(distinct ai.id) as invoice_count
from
   app-braavos.accounting_invoices ai
left join redshift.business.fact_clients_summary fcs on ai.owner_uuid = fcs.client_id
left join business.fact_invoices_summary as fis on ai.owner_uuid= fis.client_id and  ai.client_invoice_number = fis.invoice_number
inner join accounting_channels [client_credit_signals_live_test] on ([client_credit_signals_live_test].client_id = fcs.client_id)
left join redshift.public.exchange_rate fx on (ai.submitted_date = fx.date)
where
  ai.submitted_date <= current_date
  and ai.submitted_date > '2014-07-01'
  and ai.state not in ('draft','deleted')    
  and NVL(ai.doc_type_opt,'') != 'FieldTicket'
  and ai.currency in ('CAD','USD') -- odd accounts in bitcoin
  and owner_uuid not in ('66d5d6f51b1d3fc05b43b961a04cb34a8ac2a5ee', '13397b269c4ff083966a3eaf5fb93c8fb01dddd5','63c894219b574988373b8091f85009ccdf3de5fe', '32a2fb7511dca2336af65c831f677089b69bcf95','01b1f6f01629a5ad8603210accec69ed6866ad85', '96dfa57e-72ba-48ca-af91-faf1be8abb0b') -- Odd Billion $ accounts
  and not ( [client_credit_signals_live_test].is_admin or [client_credit_signals_live_test].is_dummy or fcs.is_fraud )
  and [ai.source=AccountingSoftware]
  and [owner_uuid!=Exclude_Clients]
  and [period=daterange_no_tz]
group by
 1,2
ORDER BY 1 desc, 2 desc


--select * from dim_clients where client_id = '96dfa57e-72ba-48ca-af91-faf1be8abb0b'