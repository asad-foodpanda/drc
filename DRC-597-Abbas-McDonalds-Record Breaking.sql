SELECT v.vendor_name,
  o.code as order_code,
  o.created_at_local as date_time,
  EXTRACT(HOUR FROM o.created_at_local) AS hour,
  o.status_code, 
  o.expedition_type,
  lo.rider.bag_time_in_seconds,
  ST_DISTANCE(lo.rider.customer_location_geo, lo.rider.vendor.location_geo)/1000 as distance_km,
  TRUNC(ST_DISTANCE(lo.rider.customer_location_geo, lo.rider.vendor.location_geo)/1000) AS distance_truncated,
  lo.rider.bag_time_in_seconds/60 as bag_time,
  CASE
    WHEN lo.is_preorder = FALSE THEN lo.rider.actual_delivery_time_in_seconds/60 
    END AS delivery_time,
  c.stacked_deliveries_count as stack_count,
FROM `fulfillment-dwh-production.pandata_curated.pd_orders` o 
LEFT JOIN `fulfillment-dwh-production.pandata_curated.lg_orders` lo
       ON o.code = lo.order_code
      AND o.global_entity_id = lo.global_entity_id
      AND o.created_date_utc = lo.created_date_utc
JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` oa
  ON oa.uuid = o.uuid 
 AND oa.created_date_utc = o.created_date_utc
JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts` od
  ON od.uuid = o.uuid 
 AND od.created_date_utc = o.created_date_utc
JOIN `dhh---analytics-apac.pandata_pk.sp_vendor_info` v
  ON o.vendor_code = v.vendor_code
LEFT JOIN UNNEST(o.products) p
LEFT JOIN UNNEST(lo.rider.deliveries) AS c 
       ON c.is_primary
WHERE o.global_entity_id = 'FP_PK'
  AND o.created_date_utc >= '2022-02-01'
  AND o.created_at_local BETWEEN '2022-02-12 06:00:00' AND '2022-02-13 05:00:00'
  AND v.vendor_code IN ('u4ju', 'u6km', 'u5ya')
  AND o.status_code IN (65, 621, 612)
ORDER BY 3
