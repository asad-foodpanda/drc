select v.vendor_code, v.name as vendor_name, verticals.vertical, max(case when o.is_gross_order then o.created_date_local end) as last_gross_order, max(case when o.is_valid_order then o.created_date_local end) as last_valid_order
from `fulfillment-dwh-production.pandata_curated.pd_vendors` v 
join `fulfillment-dwh-production.pandata_curated.pd_orders` o 
  on o.vendor_code = v.vendor_code and o.global_entity_id = v.global_entity_id
join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` verticals
  on verticals.vendor_code = v.vendor_code
where v.global_entity_id = 'FP_PK'
  and o.created_date_utc >= '2019-01-01'
  and lower(verticals.city) = 'sahiwal'
  and verticals.vertical = 'restaurants'

group by 1,2,3