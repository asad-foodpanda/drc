select o.created_date_local as date, 
  v.vendor_code,
  v.name as vendor_name,
  count(distinct case when o.is_valid_order and o.is_test_order = false then o.code end) as valid_orders,
  sum(case when o.is_valid_order and o.is_test_order = false then oa.gmv_local end) as gmv,
from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join `fulfillment-dwh-production.pandata_curated.pd_vendors` v
  on o.vendor_code = v.vendor_code 
  and o.global_entity_id = v.global_entity_id 
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` oa
  on oa.uuid = o.uuid and o.created_date_utc = oa.created_date_utc

where o.created_date_utc between '2020-07-01' and '2021-06-30'
  and o.global_entity_id = 'FP_PK'
  and o.vendor_code in ("S5mq","t0mj","w2nq","u3vj","t0nm","t8lt","t9kx","u6nf","t1uw","t4it","t4yd","t3kn")
group by 1,2,3