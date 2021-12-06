with customers as (
 select  pd_customer_uuid, 
--   extract(isoweek from created_date_local) as week,
--   format_date('%B', created_date_local) as month,
    created_date_local as date,
  FROM `fulfillment-dwh-production.pandata_report.marketing_pd_orders_agg_acquisition_dates`
  where
  is_first_valid_order_restaurants_delivery
  and global_entity_id = 'FP_PK'
  and created_date_local between date_trunc(date_sub(current_date, interval 6 month), month) and current_date()
),

total_customers as (
  select format_date('%B', date) as month,
  count(distinct pd_customer_uuid) as total_customers
  from customers
  group by 1
),

orders as (
  select distinct o.pd_customer_uuid, 
    date_diff(o.created_date_local, c.date, month) as month_number,
  from `fulfillment-dwh-production.pandata_curated.pd_orders` o
  left join customers c 
         on c.pd_customer_uuid = o.pd_customer_uuid
  join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
    on v.vendor_code = o.vendor_code
  where o.global_entity_id = 'FP_PK'
    and o.is_valid_order 
    and not o.is_test_order
    and v.vertical = 'restaurants'
    and o.is_own_delivery 
    and o.expedition_type = 'delivery'
    and o.created_date_utc between date_trunc(date_sub(current_date, interval 6 month), month) - 1 and current_date()
)
, retentions as (
  select format_date('%B', c.date) as month,
    o.month_number,
    count(1) as num_users
  from orders o
  left join customers c
         on o.pd_customer_uuid = c.pd_customer_uuid
  group by 1,2
)
select r.month, t.total_customers, r.month_number, r.num_users as users_retained, r.num_users / t.total_customers as retention_rate
from retentions r
left join total_customers t
       on t.month = r.month
where r.month is not null
  and r.month_number > 0
order by 1,3