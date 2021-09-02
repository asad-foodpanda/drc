SELECT
  o.code,
  v.vendor_code,
  o.created_date_local as date,
  v.name as vendor_name,
  o.delivery_fee_local,
  o.subtotal_local,
  o.total_value_local,
  o.status_code,
  o.order_comment,
  o.customer_comment,
  o.vendor_comment,
FROM
  `fulfillment-dwh-production.pandata_curated.pd_orders` o
left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v 
       on o.pd_vendor_uuid = v.uuid 
      and v.global_entity_id = o.global_entity_id
WHERE
  o.global_entity_id = 'FP_PK'
  AND v.chain_code = 'ct7xe'
  AND o.status_code IN (68)
  AND o.created_date_utc >= '2020-10-31'
  AND o.created_date_local BETWEEN '2020-11-01' AND '2021-06-30'