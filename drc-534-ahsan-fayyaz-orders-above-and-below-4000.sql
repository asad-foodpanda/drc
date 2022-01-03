with data as (
select v.vertical,
  count(distinct case when o.is_gross_order then o.code end) as gross_orders,
  count(distinct case when o.is_valid_order then o.code end) as successful_orders,
  count(distinct case when o.is_failed_order then o.code end) as cancelled_orders,
  count(distinct case when o.is_gross_order and oa.gfv_local < 4000 then o.code end) as gross_orders_below_4000,
  count(distinct case when o.is_valid_order and oa.gfv_local < 4000 then o.code end) as successful_orders_below_4000,
  count(distinct case when o.is_failed_order and oa.gfv_local < 4000 then o.code end) as cancelled_orders_below_4000,
  count(distinct case when o.status_code = 65 and oa.gfv_local < 4000 then o.code end) as cashloss_orders_below_4000,
  count(distinct case when o.is_gross_order and oa.gfv_local >= 4000 then o.code end) as gross_orders_above_4000,
  count(distinct case when o.is_valid_order and oa.gfv_local >= 4000 then o.code end) as successful_orders_above_4000,
  count(distinct case when o.is_failed_order and oa.gfv_local >= 4000 then o.code end) as cancelled_orders_above_4000,
  count(distinct case when o.status_code = 65 and oa.gfv_local >= 4000 then o.code end) as cashloss_orders_above_4000

from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` oa
  on o.uuid = oa.uuid and o.created_date_utc = oa.created_date_utc
left join `dhh---analytics-apac.pandata_pk.sp_vendor_info` v
       on v.vendor_code = o.vendor_code 
where o.created_date_utc between '2021-12-23' and '2022-01-01'
  and o.global_entity_id = 'FP_PK'
group by 1)

select *
from data
where vertical not in ('PandaGo', 'caterers')