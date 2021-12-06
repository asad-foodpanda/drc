with data_cpp as (select a.global_vendor_id as grid,
  v.vendor_code,
  v.vendor_name,
  v.city,
  ac.product,
  a.owner_name,
  coalesce(u.full_name, uh.full_name) as account_owner,
  coalesce(u.email, uh.email) as email,
  coalesce(ou.full_name, ouh.full_name) as opportunity_owner,
  coalesce(ou.email, ouh.email) as opportunity_owner_email,
  if(coalesce(u.full_name, uh.full_name) in ('kulsoom israr', 'hadi safdar', 'syed zeeshan', 'ahmed nawaz', 'adeel mushtaq', 'hashim toheed', 'ayesha riaz'), "inside_sales", "Normal") as inside_sales_check_on_account,
  if(coalesce(ou.full_name, ouh.full_name) in ('kulsoom israr', 'hadi safdar', 'syed zeeshan', 'ahmed nawaz', 'adeel mushtaq', 'hashim toheed', 'ayesha riaz'), "inside_sales", "Normal") as inside_sales_check_on_opportunity,
  a.name as account_name,
  a.type,
  a.country_name,
  date(datetime_add(a.last_modified_at_utc, interval 5 hour)) as last_modified_date,
  ac.name as additional_charge_name,
  ac.status,
  ac.start_date_local,
  ac.termination_date_local,
  ac.termination_reason,
  ac.listed_price_local,
  ac.total_amount_local,
  ac.type,
  
from `fulfillment-dwh-production.pandata_curated.sf_accounts` a
left join `fulfillment-dwh-production.pandata_curated.sf_additional_charges` ac
       on a.id = ac.sf_account_id and a.global_entity_id = ac.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.sf_opportunities` o
       on o.id = ac.sf_opportunity_id
left join `fulfillment-dwh-production.pandata_curated.sf_users` ou on o.sf_owner_id = ou.id
left join `fulfillment-dwh-production.pandata_curated.sf_users__historical` ouh on o.sf_owner_id = ouh.id
left join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
       on v.grid = a.global_vendor_id 
left join `fulfillment-dwh-production.pandata_curated.sf_users` u on a.sf_owner_id = u.id
left join `fulfillment-dwh-production.pandata_curated.sf_users__historical` uh on a.sf_owner_id = uh.id

where a.global_entity_id = 'FP_PK'
  and ac.status IN ('Charged', 'Terminated')
  and ac.start_date_local between '2021-01-01' and '2021-11-30'
  and starts_with(ac.product, 'Premium Placement')
  and (lower(ac.product) like '%premium%' or lower(ac.product) like '%organic%' or lower(ac.product) like '%organic cpc%')
order by 1),
ncr as (
select city, opportunity_owner, inside_sales_check_on_opportunity, inside_sales_check_on_account, vendor_code, vendor_name, owner_name as account_owner_name, format_date('%B %Y',start_date_local) as month, sum(ifnull(total_amount_local,0)) as total_cpc_cpp_amount_local
from data_cpp
group by 1,2,3,4,5,6,7,8)

select * from ncr