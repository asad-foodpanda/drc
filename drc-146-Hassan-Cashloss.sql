SELECT
  o.code,
  o.created_date_local,
  v.name as vendor_name,
  o.delivery_fee_local,
  o.subtotal_local,
  o.total_value_local,
  o.status_code,
  o.decline_reason.title as decline_reason
FROM
  `fulfillment-dwh-production.pandata_curated.pd_orders` o
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v on o.pd_vendor_uuid = v.uuid and v.global_entity_id = o.global_entity_id 
WHERE
  o.global_entity_id = 'FP_PK'
  AND o.vendor_code IN ('p5xl',
    'x8ne',
    'xyzm',
    'b2mb',
    'rukt',
    'mf1t')
  AND o.status_code IN (65,
    68)
  AND o.created_date_utc >= '2021-01-01'
  AND o.created_date_local BETWEEN '2021-05-01'
  AND '2021-06-29'