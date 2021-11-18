with customers as (
  select o.pd_customer_id
  from `fulfillment-dwh-production.pandata_curated.pd_orders` o
  where o.created_date_utc >= current_date() - 15
    and o.created_date_local >= current_date() - 14
    and o.global_entity_id = 'FP_PK'
    and o.is_valid_order 
    and not o.is_test_order
)
, thresholds as (
select o.pd_customer_id, 
  nth_value(o.created_at_local, 10) over (partition by o.pd_customer_id order by o.created_at_local) as ten, 
  nth_value(o.created_at_local, 20) over (partition by o.pd_customer_id order by o.created_at_local) as twenty, 
  nth_value(o.created_at_local, 30) over (partition by o.pd_customer_id order by o.created_at_local) as thirty,
  nth_value(o.created_at_local, 40) over (partition by o.pd_customer_id order by o.created_at_local) as forty, 
  nth_value(o.created_at_local, 50) over (partition by o.pd_customer_id order by o.created_at_local) as fifty, 
  nth_value(o.created_at_local, 60) over (partition by o.pd_customer_id order by o.created_at_local) as sixty
from `fulfillment-dwh-production.pandata_curated.pd_orders` o
where o.created_date_utc >= '2018-01-01'
  and o.global_entity_id = 'FP_PK'
  and o.is_valid_order
  and not o.is_test_order
  and o.pd_customer_id in (select pd_customer_id from customers)
)
select o.pd_customer_id, 
count(distinct case when o.status_code = 65 and created_at_local < t.ten then o.code end) as first_ten,
count(distinct case when o.status_code = 65 and created_at_local < t.twenty then o.code end) as first_twenty,
count(distinct case when o.status_code = 65 and created_at_local < t.thirty then o.code end) as first_thirty,
count(distinct case when o.status_code = 65 and created_at_local < t.forty then o.code end) as first_forty, 
count(distinct case when o.status_code = 65 and created_at_local < t.fifty then o.code end) as first_fifty, 
count(distinct case when o.status_code = 65 and created_at_local < t.sixty then o.code end) as first_sixty, 
count(distinct case when o.is_valid_order then o.code end) as total_sucessful_orders_to_date,
from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join thresholds t on o.pd_customer_id = t.pd_customer_id
where o.global_entity_id = 'FP_PK'
  and o.created_date_utc >= '2018-01-01'
--   and o.status_code = 65
group by 1