with new_customers as (
  select o.city_name,
--     format_date('%W', ad.created_date_local) as week,
    format_date('%B', ad.created_date_local) as month,
    count(ad.pd_customer_uuid) as new_customers,
  FROM `fulfillment-dwh-production.pandata_report.marketing_pd_orders_agg_acquisition_dates` ad
  join `fulfillment-dwh-production.pandata_curated.lg_orders` o
    on ad.order_code = o.code and ad.global_entity_id = o.global_entity_id
  where is_first_valid_order_restaurants_delivery
    and ad.global_entity_id = 'FP_PK'
    and ad.created_date_local between date_trunc(date_sub(current_date, interval 6 month), month) and current_date()
    and o.created_date_utc between date_trunc(date_sub(current_date, interval 6 month), month) - 1 and current_date()
    and o.created_date_local between date_trunc(date_sub(current_date, interval 6 month), month) and current_date()
  group by 1,2
),

unique_customers as (
  select format_date('%B', o.created_date_local) as month,
--     format_date('%W', o.created_date_local) as week,
    l.city_name,
    count(distinct o.code) as total_orders,
    count(distinct o.pd_customer_uuid) as unique_customers
  from `fulfillment-dwh-production.pandata_curated.pd_orders` o
  join `fulfillment-dwh-production.pandata_curated.lg_orders` l
    on l.code = o.code and l.global_entity_id = o.global_entity_id
  join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
    on v.vendor_code = o.vendor_code
  where o.global_entity_id = 'FP_PK'
    and o.created_date_utc between date_trunc(date_sub(current_date, interval 6 month), month) - 1 and current_date()
    and o.created_date_local between date_trunc(date_sub(current_date, interval 6 month), month) and current_date()
    and l.created_date_utc between date_trunc(date_sub(current_date, interval 6 month), month) - 1 and current_date()
    and o.is_valid_order
    and not o.is_test_order
    and o.expedition_type = 'delivery'
    and o.is_own_delivery
  group by 1,2
)

select u.month, u.city_name, u.unique_customers, c.new_customers
from unique_customers u
left join new_customers c
       on u.city_name = c.city_name 
       and u.month = c.month
--        and u.week = c.week