with may_activations as (
  select a.global_vendor_id, v.vendor_code
  FROM `fulfillment-dwh-production.pandata_curated.sf_opportunities` o
  join `fulfillment-dwh-production.pandata_curated.sf_accounts` a
    on o.global_vendor_id = a.global_vendor_id and o.global_entity_id = a.global_entity_id
  join `fulfillment-dwh-production.pandata_curated.sf_record_types` r
    on r.id = o.sf_record_type_id 
  left join `fulfillment-dwh-production.pandata_curated.pd_vendors` v
         on v.global_vendor_id = a.global_vendor_id and v.global_entity_id = a.global_entity_id
  where a.global_entity_id = 'FP_PK'
    and lower(o.business_type) IN ('new business', 'new bussiness', 'winback', 'win back')
    and lower(o.stage_name) in ('closed won', 'closedwon')
    and lower(a.type) IN ('branch - main', 'branch - virtual restaurant')
    and o.is_marked_for_testing_training = False
    and DATE(o.close_date) between '2021-05-01' and '2021-05-31'
),
not_odr_vendors as (
SELECT DISTINCT
    CASE WHEN v.chain_code = 'cb1qb' THEN 'ODR' ELSe 'Others' END as ODR_Ct,
    v.chain_code,
    v.chain_name,
    v.vendor_code,
    v.uuid,
FROM 
    `fulfillment-dwh-production.pandata_curated.pd_vendors` v
WHERE
    global_entity_id in ('FP_PK')
),

 home as (
  SELECT 
    vendor_code, f.id
  FROM 
    `fulfillment-dwh-production.pandata_curated.pd_vendors` v, unnest(food_characteristics) f
  WHERE 
    global_vendor_id in ('FP_PK')
  AND f.id = '132'
),

verticals as (
  select
  v.vendor_code,
  v.name as vendor_name,
  case 
    when odv.ODR_Ct = 'ODR' then 'PandaGo'
    when lower(b.business_type_apac) LIKE '%concept%' then 'Concepts'
    when lower(b.business_type_apac) LIKE '%kitchen%' then 'Shared Kitchens'
    when lower(a.vertical_segment) like '%home%' then 'Home Chefs'    
    when lower(a.vertical_segment) like '%darkstore%' then 'Darkstores'
    when a.vertical_segment = 'caterers' then 'Caterers'
    when lower(a.vertical_segment) = 'regular restaurant' then 'Restaurants'
    when lower(a.vertical) like '%shop%' then 'Shops'
    when a.vertical is null then
     case 
     when (v.vendor_code IN (select vendor_code from home)) or lower(b.business_type_apac) like '%home%' then 'home chefs'
     when b.business_type_apac = 'caterers' then 'Caterers'
     when b.business_type_apac = 'dmarts' then 'Darkstores'
     end
    else lower(a.vertical_segment)
    end as vertical

    from `fulfillment-dwh-production.pandata_curated.pd_vendors` v
  join `fulfillment-dwh-production.pandata_curated.sf_accounts` a
    on a.global_vendor_id = v.global_vendor_id
  join `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` b on v.uuid = b.uuid
  left join not_odr_vendors odv on v.uuid = odv.uuid
  where v.global_entity_id  ='FP_PK'),

offline AS (
SELECT 
  vendor_code,
  report_date, 
  country, 
  RO.rdbms_id, 
  SUM (closed_hours_monitor_unreachable_offline) AS closed_MUO, 
  SUM (closed_hours_vendor_device) AS closed_vendor_device, 
  SUM (closed_hours_vendor_compliance) closed_vendor_compliance,
  SUM (closed_Hours_check_in_required) closed_vendor_check_in_required,
  SUM (closed_hours_order_declined) AS closed_order_declined, 
  SUM (closed_hours_vendor_courier_delay) AS closed_vendor_courier_delay,
  SUM (closed_hours_cc_special_day_closed) AS closed_cc_special_day_closed,  
  SUM (closed_hours_cc_temporarily_closed) AS closed_cc_temporarily_closed,   
  SUM (closed_hours_churn) AS closed_churn, 
  SUM (closed_hours_other) AS closed_other, 
  SUM (closed_hours) AS closed_total, 
  SUM (open_hours) AS open_total,
FROM `dhh---analytics-apac.pandata_report.restaurant_offline`  AS RO

WHERE DATE(report_date) between '2021-01-01' and (current_date()-1) and RO.rdbms_id=12
GROUP BY 1,2,3,4),
data as (
SELECT 
    o.report_date as Report_Date,
    v.vendor_code as Vendor_Code,
    v.vendor_id as Vendor_Id,
    v.name as Vendor_Name,
    v.global_vendor_id as Grid,
    sf.restaurant_city as City,
    sf.grade as Grade,
    ver.vertical as Vertical,
    sf.status as SF_Status,
    CASE When v.is_active is TRUE THEN 'Active' ELSE 'Inactive' END as BE_Status,
    v.is_private as Private_Vendor,
safe_divide(o.closed_MUO, o.open_total) AS MUO_percent,
safe_divide(o.closed_vendor_device, o.open_total) AS vendor_device_percent,
safe_divide(o.closed_vendor_compliance,o.open_total) AS vendor_compliance_percent,
safe_divide(o.closed_vendor_check_in_required ,o.open_total) As closed_vendor_check_in_required,
safe_divide(o.closed_order_declined ,o.open_total) AS order_declined_percent,
safe_divide(o.closed_vendor_courier_delay,o.open_total) AS vendor_courier_delay_percent,
safe_divide(o.closed_cc_special_day_closed ,o.open_total) AS cc_special_day_closed_percent,  
safe_divide(o.closed_cc_temporarily_closed,o.open_total) AS cc_temporarily_closed_percent,
safe_divide(o.closed_churn, o.open_total) AS churn_percent,
safe_divide(o.closed_other,o.open_total) AS closed_other_percent,
safe_divide((IFNULL(o.closed_MUO,0)+IFNULL(o.closed_vendor_compliance,0)+IFNULL(o.closed_order_declined,0)+IFNULL(o.closed_vendor_courier_delay,0)),o.open_total) AS VM_offline_percent, 
o.open_total as total_open_hours,
o.closed_total as total_close_hours,
safe_divide(o.closed_total,o.open_total) AS closed_percent

FROM 
    `fulfillment-dwh-production.pandata_curated.pd_vendors` v
INNER JOIN 
    `fulfillment-dwh-production.pandata_curated.sf_accounts` as sf ON sf.global_vendor_id = v.global_vendor_id and sf.country_name = 'Pakistan'
INNER JOIN
    `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` b on v.uuid = b.uuid
LEFT JOIN 
     not_odr_vendors odv on v.uuid = odv.uuid
LEFT JOIN 
      offline as o ON o.vendor_code = v.vendor_code
LEFT JOIN 
      verticals as ver on ver.vendor_code = v.vendor_code
WHERE v.global_entity_id = 'FP_PK'
    AND v.is_active is true
    AND v.is_test is not true 
    AND sf.is_marked_for_testing_training is not true
    AND ver.vertical in ('Restaurants', 'Concepts', 'Shared_kitchens'))

select Report_Date, Vendor_Code, Vendor_Name, Vertical, total_open_hours from data where vendor_code in (select vendor_code from may_activations)
and Report_Date between '2021-06-07' and '2021-06-13'