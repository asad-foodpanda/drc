declare start_date date default '2020-08-01';
declare end_date date default '2021-07-31';

with orders_per_customer as (
  select vendor_code, pd_customer_uuid,  format_date('%B %Y', created_date_local) as month,
  count(distinct case when is_valid_order then code end) as valid_orders,
  from `fulfillment-dwh-production.pandata_curated.pd_orders`
  where global_entity_id = 'FP_PK'  
  and created_date_utc between start_date - 1 and end_date + 1
  and created_date_local between start_date and end_date
  and vendor_code in ('s7sa', 'u1tl')
  group by 1,2,3
)
,

reorder_customers as (
  select vendor_code,
    month, 
    count(distinct case when valid_orders > 1 then pd_customer_uuid end) as reorder_customers,
    count(distinct pd_customer_uuid) as unique_customers,
  from orders_per_customer
  group by 1,2
)

select o.vendor_code, 
  v.name as vendor_name, 
  format_date('%B %Y', o.created_date_local) as month,
  reorder_customers.reorder_customers/reorder_customers.unique_customers as reorder_rate,
  count(distinct case when is_valid_order then code end) as successful_orders,
  count(distinct case when is_gross_order then code end) as gross_orders,
  count(distinct case when 
    oa.is_first_valid_order_with_this_vendor 
    then o.pd_customer_uuid end) as 
    new_customers_with_first_order_with_this_vendor,
  count(distinct case when oa.is_first_valid_order_platform 
    then o.pd_customer_uuid end) as 
    new_customers_with_first_order_vendor_and_foodpanda,
  sum(case when is_valid_order then acc.gmv_local end) as gmv,
  
from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join `fulfillment-dwh-production.pandata_curated.pd_vendors` v
  on o.global_entity_id = v.global_entity_id and o.vendor_code = v.vendor_code
join `fulfillment-dwh-production.pandata_report.marketing_pd_orders_agg_acquisition_dates` oa
  on oa.uuid = o.uuid and o.created_date_utc = oa.created_date_utc
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` acc
  on acc.uuid = o.uuid and o.created_date_utc = acc.created_date_utc
left join reorder_customers 
    on reorder_customers.vendor_code = v.vendor_code 
    and reorder_customers.month = format_date('%B %Y', o.created_date_local)
where o.global_entity_id = 'FP_PK'
  and o.created_date_utc between start_date - 1 and end_date + 1
  and o.created_date_local between start_date and end_date
  and o.vendor_code in ('s7sa', 'u1tl')
group by 1,2,3,4
order by 1,2