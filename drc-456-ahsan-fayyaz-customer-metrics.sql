with data as (
select o.pd_customer_id as customer_id, 
  count(distinct case when o.is_gross_order then o.code end) as gross_orders,
  count(distinct case when o.is_valid_order then o.code end) as successful_orders,
  count(distinct case when o.is_valid_order and o.subtotal_local >= 2000 then o.code end) as successful_orders_above_2000,
  count(distinct case when o.is_valid_order and o.subtotal_local >= 4000 then o.code end) as successful_orders_above_4000,
  sum(case when o.is_valid_order then oa.gmv_local end) as lifetime_gmv, 
  count(distinct case when o.status_code = 65 then o.code end) as cashloss_order_count,
  sum(case when o.status_code = 65 then o.subtotal_local end) as cashloss_amount,
  count(distinct case when o.status_code = 65 and o.subtotal_local >= 2000 then o.code end) as cashloss_orders_above_2000,
  count(distinct case when o.status_code = 65 and o.subtotal_local >= 3000 then o.code end) as cashloss_orders_above_3000,
  count(distinct case when o.status_code = 65 and o.subtotal_local >= 4000 then o.code end) as cashloss_orders_above_4000,
  count(distinct case when o.is_valid_order and ov.is_voucher_used then o.code end) as voucher_orders,
  sum(case when o.is_valid_order then ov.voucher.value_local end) as voucher_amount,
  avg(case when o.is_valid_order then oa.gmv_local end) as avg_basket_size,
  count(distinct case when o.is_valid_order and o.payment_type.title = 'Cash On Delivery' then o.code end) as cod_orders,
  count(distinct case when o.is_valid_order and o.payment_type.title = 'Online Payment' then o.code end) as online_payment_orders,
  count(distinct case when o.is_valid_order and o.payment_type.title = 'Balance' then o.code end) as balance_payment_orders,
  count(distinct case when o.is_valid_order and o.payment_type.title = 'No Payment' then o.code end) as no_payment_orders,
  count(distinct case when o.is_valid_order and o.payment_type.title = 'invoice' then o.code end) as invoice_orders,
  count(distinct case when o.is_valid_order and cp.is_corporate_order then o.code end) as corporate_orders,
  count(distinct case when o.is_valid_order and o.payment_type.title = 'invoice' and cp.is_corporate_order then o.code end) as corporate_invoice_orders,
  min(case when o.is_gross_order then o.created_date_local end) as first_order_date,
  max(case when o.is_gross_order then o.created_date_local end) as last_order_date,
  
  
from `fulfillment-dwh-production.pandata_curated.pd_orders` o
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` oa
  on o.uuid = oa.uuid and o.created_date_utc = oa.created_date_utc
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers` ov
  on ov.uuid = o.uuid and o.created_date_utc = ov.created_date_utc
join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_cp_orders` cp
  on cp.uuid = o.uuid and o.created_date_utc = cp.created_date_utc
-- left join `fulfillment-dwh-production.pandata_curated.pd_customers` c
--        on c.uuid = o.pd_customer_uuid 
where o.created_date_utc >= '2017-09-01'
  and o.global_entity_id = 'FP_PK'
group by 1)

select *, date_diff(first_order_date, last_order_date, day) as customer_age 
from data
  