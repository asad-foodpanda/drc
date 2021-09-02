with orders as (select o.vendor_code, format_date('%B %Y', o.created_date_local) as month, 
  count(distinct case when o.is_valid_order then o.code end) as successful_orders, 
  sum(case when o.is_valid_order then a.gmv_local end) as gmv,
  count(case when oac.is_first_valid_order_with_this_chain then o.pd_customer_uuid end) as new_customers_acquired
  
from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
  on o.vendor_code = v.vendor_code
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` a
  on a.uuid = o.uuid
left join `fulfillment-dwh-production.pandata_report.marketing_pd_orders_agg_acquisition_dates` oac
    on oac.uuid = o.uuid and oac.created_date_utc >= '2020-07-31'
where o.created_date_utc >= '2020-07-31'
  and a.created_date_utc >= '2020-07-31'
  and o.created_date_local between '2020-08-01' and '2021-07-31'
  and v.chain_code = 'cw2tn'
  and o.global_entity_id = 'FP_PK'
group by 1,2),

month_raw as (
  select v.vendor_code,
    o.pd_customer_uuid,
    format_date('%B %Y', o.created_date_local) month,
    count(distinct case when o.is_valid_order then o.code end) as orders
from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
  on o.vendor_code = v.vendor_code and o.global_entity_id = 'FP_PK'
where o.created_date_utc >= '2020-07-31'
  and o.global_entity_id = 'FP_PK'
  and o.created_date_local BETWEEN '2020-08-01' and '2021-07-31'
group by 1,2,3),

monthly_final as (
  select vendor_code,
    month,
    count(distinct pd_customer_uuid) as unique_customers,
    count(distinct case when orders > 1 then pd_customer_uuid end) as reorder_customers
from month_raw
group by 1,2),

data as (
  select o.month, 
    o.vendor_code,
    o.successful_orders,
    o.gmv,
    m.reorder_customers/m.unique_customers as reorder_rate,
    o.new_customers_acquired
  from orders o
  left join monthly_final m
         on o.vendor_code = m.vendor_code 
        and o.month = m.month
)

select * from data
