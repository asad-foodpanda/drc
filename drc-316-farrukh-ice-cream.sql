with data as (select
v.vendor_code as vendor_code,
verticals.grid,
v.name as vendor_name, 
verticals.vertical,
verticals.city,
verticals.zone_name,
count(distinct o.code) as total_orders,
from `fulfillment-dwh-production.pandata_curated.pd_orders` as o
join `fulfillment-dwh-production.pandata_curated.pd_vendors` as v
  on o.vendor_code = v.vendor_code
left join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` verticals
       on verticals.vendor_code = v.vendor_code
and o.global_entity_id = v.global_entity_id
where
o.created_date_utc >= current_date()-32
and o.created_date_local >= current_date()-31
and o.global_entity_id = 'FP_PK'
and o.is_valid_order = true
and o.is_test_order = false
and v.is_test = false
and verticals.is_sf_test = false
and regexp_contains(lower(v.name),
r'baskin robbins|alletto|peshawari ice cream|ice land|movenpick ice cream|jay[_ -]?bee|omore|ice cream|lush[- _]?crush|ice[- _]?bear')

and not regexp_contains(lower(v.name), r'juice[- _]*land|rice[_ -]land')

group by 1,2,3,4,5,6

)
select * from data
 

