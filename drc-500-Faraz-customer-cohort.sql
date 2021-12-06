with customers as (
 select  pd_customer_uuid, 
  extract(isoweek from created_date_local) as week,
  format_date('%B', created_date_local) as month,
  FROM `fulfillment-dwh-production.pandata_report.marketing_pd_orders_agg_acquisition_dates`
  where
  is_first_valid_order_restaurants_delivery
  and global_entity_id = 'FP_PK'
  and created_date_local between date_trunc(date_sub(current_date, interval 6 month), month) and current_date()
),

total_customers as (
  select month,
  count(distinct pd_customer_uuid) as total_customers
  from customers
  group by 1
), data as (

select c.month as customer_cohort_month, t.total_customers as total_customers_in_cohort, format_date('%B', o.created_date_local) as order_month, count(distinct o.code) as orders 
from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join customers c 
  on c.pd_customer_uuid = o.pd_customer_uuid
join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
  on v.vendor_code = o.vendor_code
join total_customers t
  on t.month = c.month
where o.global_entity_id = 'FP_PK'
  and o.created_date_utc >= date_trunc(date_sub(current_date, interval 6 month), month) - 1
  and o.created_date_local >= date_trunc(date_sub(current_date, interval 6 month), month)
  and o.is_valid_order 
  and not o.is_test_order
  and v.vertical = 'restaurants'
  and o.expedition_type = 'delivery'
  and o.is_own_delivery
group by 1,2,3)

select *, orders/ total_customers_in_cohort as orders_per_customer
from data