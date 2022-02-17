WITH 
max_date1 as (select o.code, o.status_code as order_status_code, status_flows.uuid, status_flows.code as status_code, datetime_add(status_flows.created_at_utc, INTERVAL 5 hour) as time
from `fulfillment-dwh-production.pandata_curated.pd_orders` as o, unnest(status_flows) as status_flows
where 
o.global_entity_id = 'FP_PK'
and created_date_utc >= '2022-01-01'
and status_flows.code IS NOT NULL),

max_date2 as(
select distinct m.code, m.order_status_code, FIRST_VALUE(m.status_code) OVER (PARTITION BY m.code ORDER BY m.uuid DESC) as status_code,
FIRST_VALUE(m.time) OVER (PARTITION BY m.code ORDER BY m.uuid DESC) as time_local,
date(FIRST_VALUE(m.time) OVER (PARTITION BY m.code ORDER BY m.uuid DESC)) as Max_date
from max_date1 m
),

Manual_NCP as
(Select distinct
m.Max_Date,
o.expedition_type,
o.is_own_delivery,
o.vendor_code,
o.vendor_name,
b.chain_code,
b.chain_name,
o.code as order_code,
date(o.expected_delivery_at_local) as expected_delivery_date,
p.title,
p.description,
Case when b.chain_code in ('cs1zc','cg4sx','co8un') and (lower(trim(p.title)) like '%celebration deal 2%' and p.price_local = 699) then 200
when b.chain_code in ('cs1zc','cg4sx','co8un') and (lower(trim(p.title)) like '%celebration deal 3%' and p.price_local = 999) then 200
when b.chain_code in ('cw4xt') and (lower(trim(p.title)) like '%aos-1571528%' and p.price_local = 899) 
or (lower(trim(p.title)) like '%celebration deal 3%' and p.price_local = 899) then 150
when b.chain_code in ('cw4xt') and (lower(trim(p.title)) like '%new celebration deal 3%' and p.price_local = 925) then 124
when (lower(trim(p.title)) like '%mcchicken & drink%' and b.vendor_code = 's4iz' and p.price_local = 264.6) then 61/1.13
when (lower(trim(p.title)) like '%mcchicken & drink%' and b.vendor_code = 's1jo' and p.price_local = 257.76) then 61/1.16
when (lower(trim(p.title)) like '%wrap & drink%' and b.vendor_code = 's1jo' and p.price_local = 301.72) then 60/1.16
when (lower(trim(p.title)) like '%wrap & drink%' and b.vendor_code = 's4iz' and p.price_local = 309.74) then 60/1.13
end as FP_Share,
0 as V_Share,
p.price_local as Cust_Price,
sum(quantity) as quantity

from 
(`fulfillment-dwh-production.pandata_curated.pd_orders` o
left join unnest(products) p)
join `fulfillment-dwh-production.pandata_curated.pd_vendors` b
on b.uuid = o.pd_vendor_uuid
join max_date2 m
on m.code = o.code
and o.global_entity_id = 'FP_PK'
left join `dhh---analytics-apac.pandata_pk.sp_vendor_info` vert on vert.vendor_code = b.vendor_code
LEFT JOIN `dhh---analytics-apac.pandata_pk.cities_exceptions` ce ON ce.city = vert.city
LEFT JOIN `dhh---analytics-apac.pandata_pk.vendors_exceptions` ve ON ve.brand_grid = vert.brand_grid
LEFT JOIN `dhh---analytics-apac.pandata_pk.vendors_exceptions` ve2 ON ve2.vendor_code = vert.vendor_code
LEFT JOIN `dhh---analytics-apac.pandata_pk.vendors_exceptions` ve3 ON ve3.group_grid = vert.group_grid
LEFT JOIN `dhh---analytics-apac.pandata_pk.zones_exceptions` ze ON CONCAT(ze.city,ze.zone) = CONCAT(vert.city,vert.zone_name)

where
o.global_entity_id = 'FP_PK'
and o.created_date_local >= '2022-01-01'
and o.created_date_utc >= '2022-01-01'
and m.Max_Date >= '2022-01-01'
-- and (
-- (ce.cashloss is false AND vert.vertical in ('restaurants') AND o.expedition_type in ('delivery','pickup') AND o.status_code = 65) or
-- (ve.cashloss is false AND o.expedition_type in ('delivery','pickup') AND o.status_code = 65) or
-- (ve2.cashloss is false AND o.expedition_type in ('delivery','pickup') AND o.status_code = 65) or
-- (ve3.cashloss is false AND o.expedition_type in ('delivery','pickup') AND o.status_code = 65) or
-- (o.expedition_type = 'delivery' AND o.status_code = 621) or
-- (ze.cashloss is false AND vert.vertical in ('restaurants') AND o.status_code = 65) or
-- (o.expedition_type = 'pickup' AND o.status_code = 612)
-- )
and
((o.expedition_type = 'delivery' AND o.status_code = 621) or
(o.expedition_type = 'pickup' AND o.status_code = 612)
or o.status_code = 65)
and (
--- dominoes addition
(b.chain_code in ('cs1zc','cg4sx','co8un')
and ((lower(trim(p.title)) like '%celebration deal 2%' and p.price_local = 699)
or (lower(trim(p.title)) like '%celebration deal 3%' and p.price_local  = 999)))
or  
--- mcdonalds addition
((lower(trim(p.title)) like '%mcchicken & drink%' and b.vendor_code = 's4iz' and p.price_local = 264.6)
or (lower(trim(p.title)) like '%mcchicken & drink%' and b.vendor_code = 's1jo' and p.price_local = 257.76)
or (lower(trim(p.title)) like '%wrap & drink%' and b.vendor_code = 's1jo' and p.price_local = 301.72)
or (lower(trim(p.title)) like '%wrap & drink%' and b.vendor_code = 's4iz' and p.price_local = 309.74))
or
---broadway addition 
(b.chain_code in ('cw4xt')
and ((lower(trim(p.title)) like '%aos-1571528%' and p.price_local = 899)
or (lower(trim(p.title)) like '%celebration deal 3%' and p.price_local = 899)
or (lower(trim(p.title)) like '%new celebration deal 3%' and p.price_local = 925)))
)


group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14),

--- NCP Portion
unique_translation as
(Select
id as pd_object_id,
title,
description,
translations,
split(split(translations,"ur_PK")[safe_offset(1)],'"')[safe_offset(2)] as object_text

from `fulfillment-dwh-production.pandata_curated.pd_products`

where
global_entity_id = 'FP_PK'
and split(split(translations,"ur_PK")[safe_offset(1)],'"')[safe_offset(2)] like '%(NCP)%')
,

Auto_NCP2 as
(
Select distinct
m.Max_Date,
o.code as order_code,
o.expedition_type,
o.is_own_delivery,
o.vendor_code,
o.vendor_name,
b.chain_code,
b.chain_name,
p.title,
p.description,
date(o.expected_delivery_at_local) as expected_delivery_date,
cast(rpad(substr(tr.object_text,strpos(tr.object_text,'*')+1),(length(substr(tr.object_text,strpos(tr.object_text,'*')+1))-length(substr(tr.object_text,strpos(tr.object_text,'@'))))) as float64) as FP_Share,
cast(regexp_extract(substr(tr.object_text, strpos(tr.object_text,'@')+1, (strpos(tr.object_text,'#')-(strpos(tr.object_text,'@')+1))), '[0-9.]+') as float64) AS V_Share,
cast(substr(tr.object_text,strpos(tr.object_text,'#')+1) as float64) as Cust_Price,
lpad(tr.object_text,18) as Op_ID,
ifnull(safe_cast(rpad(substr(tr.object_text,strpos(tr.object_text,'*')+1),(length(substr(tr.object_text,strpos(tr.object_text,'*')+1))-length(substr(tr.object_text,strpos(tr.object_text,'@'))))) as float64),0) +
ifnull(cast(regexp_extract(substr(tr.object_text, strpos(tr.object_text,'@')+1, (strpos(tr.object_text,'#')-(strpos(tr.object_text,'@')+1))), '[0-9.]+') as float64) ,0) as Total_Discount,
sum(quantity) as quantity

from unique_translation tr 
join (`fulfillment-dwh-production.pandata_curated.pd_orders` o
left join unnest(products) p)
on tr.pd_object_id = p.pd_product_id
join `fulfillment-dwh-production.pandata_curated.pd_vendors` b
on b.uuid = o.pd_vendor_uuid
join max_date2 m
on m.code = o.code
and o.global_entity_id = 'FP_PK'
left join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` vert on vert.vendor_code = b.vendor_code
LEFT JOIN `dhh---analytics-apac.pandata_pk.cities_exceptions` ce ON ce.city = vert.city
LEFT JOIN `dhh---analytics-apac.pandata_pk.vendors_exceptions` ve ON ve.brand_grid = vert.brand_grid
LEFT JOIN `dhh---analytics-apac.pandata_pk.vendors_exceptions` ve2 ON ve2.vendor_code = vert.vendor_code
LEFT JOIN `dhh---analytics-apac.pandata_pk.vendors_exceptions` ve3 ON ve3.group_grid = vert.group_grid
LEFT JOIN `dhh---analytics-apac.pandata_pk.zones_exceptions` ze ON CONCAT(ze.city,ze.zone) = CONCAT(vert.city,vert.zone_name)

where
o.global_entity_id = 'FP_PK'
and o.created_date_local >= '2022-01-01'
and o.created_date_utc >= '2022-01-01'
and m.Max_Date >= '2022-01-01'
and cast(substr(tr.object_text,strpos(tr.object_text,'#')+1) as float64) = p.price_local
and tr.object_text like '%(NCP)%'
and p.title not like '%(NCP)%'
and
((o.expedition_type = 'delivery' AND o.status_code = 621) or
(o.expedition_type = 'pickup' AND o.status_code = 612)
or o.status_code = 65)
and tr.object_text like '%(NCP)%*%@%#%'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
),

Combined_Auto_Manual as
(Select distinct
max_date as date,
expedition_type as Exp_type,
is_own_delivery  as Delivery_type,
vendor_code  as vendor_code,
vendor_name  as vendor_name,
chain_code  as chain_code,
chain_name  as chain_name,
order_code  as order_code,
expected_delivery_date,
title  as product_title,
description as product_description,
FP_Share as FP_Share,
V_Share,
Cust_Price,
Quantity  as Quantity,

from
Auto_NCP2 aN
union all
(Select * from Manual_NCP mN)),

product_level_data2 as
(Select distinct *  from Combined_Auto_Manual),

-- below added on 10/02/2022
product_level_data3 as (select *, 
  
from product_level_data2
qualify row_number() over(partition by date
,Exp_type 
,Delivery_type
,vendor_code
,vendor_name
,chain_code
,chain_name
,order_code
,expected_delivery_date
,product_title 
,product_description
-- ,CAST(FP_Share as STRING)
,CAST(Cust_Price as STRING)
,Quantity
order by V_Share desc) =1
 ) 


Select
date(Date) as Date,
order_code,
product_title,
product_description,
vendor_code,
vendor_name,
chain_name,
chain_code,
Exp_type,
Delivery_type,
expected_delivery_date,
Cust_Price,
sum(Quantity) as Quantity,
sum(Quantity*FP_Share) as Cash_Back,
sum(Quantity*V_Share) as Vendor_Share

from product_level_data3
WHERE date between '2022-02-01' and '2022-02-16'
  AND chain_code IN ('ca5vf', 'co8ie')
group by 1,2,3,4,5,6,7,8,9,10,11,12


