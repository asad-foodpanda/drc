select v.chain_code, v.chain_name, v.vendor_code, v.name as vendor_name, format_date('%B %Y', o.created_date_local) as month, sum(case when o.is_valid_order then oa.gfv_local end) as gfv  from `fulfillment-dwh-production.pandata_curated.pd_vendors` v
join `fulfillment-dwh-production.pandata_curated.pd_orders` o on o.pd_vendor_uuid = v.uuid
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` oa on o.uuid = oa.uuid
where o.created_date_local between '2020-05-01' and '2021-05-31' 
  and oa.created_date_local between '2020-05-01' and '2021-05-31'
  and o.created_date_utc >= '2020-01-01'
  and oa.created_date_utc >= '2020-01-01'
  and v.global_entity_id = 'FP_PK'
  and v.chain_code in ('cs1zc', 'cg4sx')
group by 1,2,3,4,5