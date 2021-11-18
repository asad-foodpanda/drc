select format_date('Week %V %Y', o.created_date_local) as week,
count (distinct case when o.is_gross_order and a.gmv_local between 0 and 1000 then o.code end) as gross_orders_0_1000,
count (distinct case when o.is_gross_order and a.gmv_local between 1001 and 2000 then o.code end) as gross_orders_1001_2000,
count (distinct case when o.is_gross_order and a.gmv_local between 2001 and 3000 then o.code end) as gross_orders_2001_3000,
count (distinct case when o.is_gross_order and a.gmv_local between 3001 and 4000 then o.code end) as gross_orders_3001_4000,
count (distinct case when o.is_gross_order and a.gmv_local between 4001 and 5000 then o.code end) as gross_orders_4001_5000,
count (distinct case when o.is_gross_order and a.gmv_local between 5001 and 6000 then o.code end) as gross_orders_5001_6000,
count (distinct case when o.is_gross_order and a.gmv_local between 6001 and 7000 then o.code end) as gross_orders_6001_7000,
count (distinct case when o.is_gross_order and a.gmv_local > 7000 then o.code end) as gross_orders_above_7000,

count (distinct case when o.is_valid_order and a.gmv_local between 0 and 1000 then o.code end) as successful_orders_0_1000,
count (distinct case when o.is_valid_order and a.gmv_local between 1001 and 2000 then o.code end) as successful_orders_1001_2000,
count (distinct case when o.is_valid_order and a.gmv_local between 2001 and 3000 then o.code end) as successful_orders_2001_3000,
count (distinct case when o.is_valid_order and a.gmv_local between 3001 and 4000 then o.code end) as successful_orders_3001_4000,
count (distinct case when o.is_valid_order and a.gmv_local between 4001 and 5000 then o.code end) as successful_orders_4001_5000,
count (distinct case when o.is_valid_order and a.gmv_local between 5001 and 6000 then o.code end) as successful_orders_5001_6000,
count (distinct case when o.is_valid_order and a.gmv_local between 6001 and 7000 then o.code end) as successful_orders_6001_7000,
count (distinct case when o.is_valid_order and a.gmv_local > 7000 then o.code end) as successful_orders_above_7000,

count (distinct case when o.status_code = 65 and a.gmv_local between 0 and 1000 then o.code end) as cashloss_orders_0_1000,
count (distinct case when o.status_code = 65 and a.gmv_local between 1001 and 2000 then o.code end) as cashloss_orders_1001_2000,
count (distinct case when o.status_code = 65 and a.gmv_local between 2001 and 3000 then o.code end) as cashloss_orders_2001_3000,
count (distinct case when o.status_code = 65 and a.gmv_local between 3001 and 4000 then o.code end) as cashloss_orders_3001_4000,
count (distinct case when o.status_code = 65 and a.gmv_local between 4001 and 5000 then o.code end) as cashloss_orders_4001_5000,
count (distinct case when o.status_code = 65 and a.gmv_local between 5001 and 6000 then o.code end) as cashloss_orders_5001_6000,
count (distinct case when o.status_code = 65 and a.gmv_local between 6001 and 7000 then o.code end) as cashloss_orders_6001_7000,
count (distinct case when o.status_code = 65 and a.gmv_local > 7000 then o.code end) as cashloss_orders_above_7000,

from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` a
  on a.uuid = o.uuid and a.created_date_utc >= '2021-01-01' - 1
where o.created_date_utc >= '2021-01-01' - 1
  and o.created_date_local >= '2021-01-01'
  and not o.is_test_order
  and o.global_entity_id = 'FP_PK'
group by 1