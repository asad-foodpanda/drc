select v.city,
  o.created_date_local as date, 
  time_trunc(time(o.created_at_local), hour) as hour,
  count(distinct o.code) as number_of_orders,
  sum(ac.gfv_local) as gfv_local
from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
  on o.vendor_code = v.vendor_code
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` ac
  on ac.uuid = o.uuid and ac.created_date_utc between '2020-07-30' and '2020-08-04'
where o.global_entity_id = 'FP_PK'
  and o.is_valid_order 
  and not o.is_test_order 
  and o.created_date_local in ('2020-07-31', '2020-08-01', '2020-08-02')
  and o.created_date_utc between '2020-07-30' and '2020-08-04'
  and v.vertical = 'restaurants'
group by 1,2,3