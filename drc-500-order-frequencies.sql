with data as (select format_date ('%B', created_date_local) as month,
  v.city,
  count(distinct pd_customer_uuid) as unique_customers,
  count(distinct o.code) as orders
from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
  on v.vendor_code = o.vendor_code
where o.global_entity_id = 'FP_PK'
  and o.created_date_utc >= date_trunc(date_sub(current_date, interval 6 month), month) - 1
  and o.created_date_local >= date_trunc(date_sub(current_date, interval 6 month), month)
  and o.is_valid_order 
  and not o.is_test_order
  and v.vertical = 'restaurants'
  and o.expedition_type = 'delivery'
  and o.is_own_delivery
  and v.city in ('Karachi', 'Lahore', 'Islamabad', 'Faisalabad', 'Rawalpindi', 'Multan')
group by 1,2)

select *, orders/unique_customers as orders_per_customer
from data