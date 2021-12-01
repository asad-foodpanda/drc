with customer_data as (
  select o.pd_customer_uuid,
        # gross_orders
    count(case when o.payment_type.title = 'Cash On Delivery' 
      and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 1 hour)  then o.code end) 
    as past_1_hour_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and
      o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 3 hour)  then o.code end) 
    as past_3_hour_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 24 hour)  then o.code end) as past_24_hour_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 48 hour)  then o.code end) as past_48_hour_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 72 hour)  then o.code end) as past_72_hour_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 168 hour)  then o.code end) as past_168_hour_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 6 month)  then o.code end) as past_6_month_orders,
    
    #failed orders
    count(case when o.payment_type.title = 'Cash On Delivery' 
      and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 1 hour) and o.is_failed_order then o.code end) 
    as past_1_hour_failed_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and
      o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 3 hour) and o.is_failed_order then o.code end) 
    as past_3_hour_failed_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 24 hour) and o.is_failed_order then o.code end) as past_24_hour_failed_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 48 hour) and o.is_failed_order then o.code end) as past_48_hour_failed_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 72 hour) and o.is_failed_order then o.code end) as past_72_hour_failed_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 168 hour)  and o.is_failed_order then o.code end) as past_168_hour_failed_orders,
    count(case when o.payment_type.title = 'Cash On Delivery' and o.created_at_local > datetime_sub('2021-11-24 23:59:59', Interval 6 month) and o.is_failed_order then o.code end) as past_6_month_failed_orders,
    
    # lifetime_stats
    count(o.code) as lifetime_orders,
    count(case when o.is_valid_order then o.code end) as successful_orders,
    sum(case when o.is_valid_order then oa.gmv_local end) as lifetime_gmv,
    count(case when o.status_code = 65 then o.code end) as cashloss_orders,
    sum(case when o.status_code = 65 then oa.gmv_local end) as cashloss_order_value,
    count(case when o.status_code = 65  and oa.gmv_local >= 2000 then o.code end) as cashloss_orders_above_2000,
    count(case when o.status_code = 65  and oa.gmv_local >= 3000 then o.code end) as cashloss_orders_above_3000,
    count(case when o.status_code = 65  and oa.gmv_local >= 4000 then o.code end) as cashloss_orders_above_4000,
    count(case when o.is_valid_order and ov.is_voucher_used then o.code end) as vouchers_claimed,
    min(case when o.is_valid_order then o.created_date_local end) as first_order_date,
    avg(case when o.is_valid_order then oa.gmv_local end) as avg_basket_size,
    count(case when o.is_valid_order and oa.gmv_local >= 2000 then o.code end) as successful_orders_above_2000,
    count(case when o.is_valid_order and oa.gmv_local >= 4000 then o.code end) as successful_orders_above_4000,
    count(case when o.payment_type.title = 'Cash On Delivery' then o.code end) as cod_orders,
    count(case when o.payment_type.title = 'Balance' then o.code end) as balance_orders,
    count(case when o.payment_type.title = 'Online Payment' then o.code end) as online_paid_orders,
    count(case when o.payment_type.title = 'invoice' then o.code end) as invoice_orders,
    count(case when o.payment_type.title = 'No Payment' then o.code end) as no_payment_orders,
    
  from `fulfillment-dwh-production.pandata_curated.pd_orders` o
  join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` oa
    on oa.uuid = o.uuid and o.created_date_utc = oa.created_date_utc
  join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers` ov
    on ov.uuid = o.uuid and ov.created_date_utc = o.created_date_utc
  left join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
        on v.vendor_code = o.vendor_code
  where o.global_entity_id = 'FP_PK'
    and o.created_date_utc between '2018-01-01' and '2021-11-25'
    and o.is_gross_order
    and not o.is_test_order
    and v.is_sf_test = false
    and not v.is_backend_test
  group by 1
),

vendor_data as (
  select o.vendor_code, 
    case when v.is_sam then 'AAA' when v.key_account_sub_category is not null then 'Key Account' else 'GI' end as vendor_category,
    v.activation_date,
    count(o.code) as lifetime_orders,
    count(case when o.is_valid_order then o.code end) as successful_orders,
    sum(case when o.is_valid_order then oa.gmv_local end) as lifetime_gmv,
    count(case when o.status_code = 65 then o.code end) as cashloss_orders,
    sum(case when o.status_code = 65 then oa.gmv_local end) as cashloss_order_value,
    count(case when o.status_code = 65  and oa.gmv_local >= 2000 then o.code end) as cashloss_orders_above_2000,
    count(case when o.status_code = 65  and oa.gmv_local >= 3000 then o.code end) as cashloss_orders_above_3000,
    count(case when o.status_code = 65  and oa.gmv_local >= 4000 then o.code end) as cashloss_orders_above_4000,
    count(case when o.is_valid_order and ov.is_voucher_used then o.code end) as vouchers_claimed,
    avg(case when o.is_valid_order then oa.gmv_local end) as avg_basket_size,
    count(case when o.is_valid_order and oa.gmv_local >= 2000 then o.code end) as successful_orders_above_2000,
    count(case when o.is_valid_order and oa.gmv_local >= 4000 then o.code end) as successful_orders_above_4000,
    count(case when o.payment_type.title = 'Cash On Delivery' then o.code end) as cod_orders,
    count(case when o.payment_type.title = 'Balance' then o.code end) as balance_orders,
    count(case when o.payment_type.title = 'Online Payment' then o.code end) as online_paid_orders,
    count(case when o.payment_type.title = 'invoice' then o.code end) as invoice_orders,
    count(case when o.payment_type.title = 'No Payment' then o.code end) as no_payment_orders,
    
  from `fulfillment-dwh-production.pandata_curated.pd_orders` o
  join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` oa
    on oa.uuid = o.uuid and o.created_date_utc = oa.created_date_utc
  join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers` ov
    on ov.uuid = o.uuid and ov.created_date_utc = o.created_date_utc
  left join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
        on v.vendor_code = o.vendor_code
  where o.global_entity_id = 'FP_PK'
    and o.created_date_utc between '2018-01-01' and '2021-11-25'
    and o.is_gross_order
    and not o.is_test_order
    and v.is_sf_test = false
    and not v.is_backend_test
  group by 1,2,3
)

select o.code as order_id, 
  o.vendor_code,
  o.created_date_local as date,
  o.created_at_local as time,
  v.vertical,
  oa.gmv_local,
  o.payment_type.title as payment_type,
  ov.is_voucher_used,
  if(o.status_code = 65, 'True', 'False') as is_cashloss,
  case 
    when o.expedition_type = 'pickup' then 'pickup'
    when o.is_own_delivery then 'OD'
    else 'VD'
  end as OD_VD,
  o.decline_reason.title as decline_reason_title,
  o.decline_reason.code as decline_reason_code,
  o.status_code,
  #customer data
  o.pd_customer_id,
  c.past_1_hour_orders,
  c.past_3_hour_orders,
  c.past_24_hour_orders,
  c.past_48_hour_orders,
  c.past_72_hour_orders,
  c.past_168_hour_orders,
  c.past_6_month_orders,
  c.past_1_hour_failed_orders,
  c.past_3_hour_failed_orders,
  c.past_24_hour_failed_orders,
  c.past_48_hour_failed_orders,
  c.past_72_hour_failed_orders,
  c.past_168_hour_failed_orders,
  c.past_6_month_failed_orders,
  c.lifetime_orders,
  c.successful_orders,
  c.lifetime_gmv,
  c.cashloss_orders,
  c.cashloss_order_value,
  c.cashloss_orders_above_2000,
  c.cashloss_orders_above_3000,
  c.cashloss_orders_above_4000,
  c.vouchers_claimed,
  c.avg_basket_size,
  c.successful_orders_above_2000,
  c.successful_orders_above_4000,
  c.cod_orders,
  c.balance_orders,
  c.online_paid_orders,
  c.invoice_orders,
  c.no_payment_orders,
  safe_divide(c.successful_orders, c.lifetime_orders) as success_rate,
  safe_divide(c.cashloss_orders, c.lifetime_orders) as cashloss_rate,
  safe_divide(c.vouchers_claimed, c.successful_orders) as voucher_rate,
  date_diff('2021-11-24',  c.first_order_date, day) as customer_age_in_days,
  vendor.lifetime_orders as vendor_lifetime_orders,
  vendor.successful_orders as vendor_successful_orders,
  vendor.lifetime_gmv as vendor_lifetime_gmv,
  vendor.cashloss_orders as vendor_cashloss_orders,
  vendor.cashloss_order_value as vendor_cashloss_order_value,
  vendor.cashloss_orders_above_2000 as vendor_cashloss_orders_above_2000,
  vendor.cashloss_orders_above_3000 as vendor_cashloss_orders_above_3000,
  vendor.cashloss_orders_above_4000 as vendor_cashloss_orders_above_4000,
  vendor.vouchers_claimed as vendor_vouchers_claimed,
  vendor.avg_basket_size as vendor_avg_basket_size,
  vendor.successful_orders_above_2000 as vendor_successful_orders_above_2000,
  vendor.successful_orders_above_4000 as vendor_successful_orders_above_4000,
  vendor.cod_orders as vendor_cod_orders,
  vendor.balance_orders as vendor_balance_orders,
  vendor.online_paid_orders as vendor_online_paid_orders,
  vendor.invoice_orders as vendor_invoice_orders,
  vendor.no_payment_orders as vendor_no_payment_orders,
  safe_divide(vendor.successful_orders, vendor.lifetime_orders) as vendor_success_rate,
  safe_divide(vendor.cashloss_orders, vendor.lifetime_orders) as vendor_cashloss_rate,
  safe_divide(vendor.vouchers_claimed, vendor.successful_orders) as vendor_voucher_rate,
  date_diff('2021-11-24',  vendor.activation_date, day) as vendor_age_in_days,
from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` oa
  on oa.uuid = o.uuid and o.created_date_utc = oa.created_date_utc
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers` ov
  on ov.uuid = o.uuid and ov.created_date_utc = o.created_date_utc
left join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` v
      on v.vendor_code = o.vendor_code
left join customer_data c on c.pd_customer_uuid = o.pd_customer_uuid 
left join vendor_data vendor on vendor.vendor_code = o.vendor_code
where o.created_date_utc between '2021-11-17' - 1 and '2021-11-25'
  and o.global_entity_id = 'FP_PK'
  and o.created_date_local between  '2021-11-17' and '2021-11-24'