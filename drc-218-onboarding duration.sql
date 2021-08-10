with activations as (
select distinct
  a.restaurant_city city,
  o.global_vendor_id as GRID,
  v.vendor_code,
  a.restaurant_city as restaurant_city,
  o.id as opportunity_id,
  a.name AS account_name,
  o.name AS opportunity_name,
  o.stage_name,
  o.close_date as opportunity_close_date,
  format_date('%b %Y', date(case when date(o.close_date) between '2021-06-01' and '2021-06-03' then '2021-05-31' else date(o.close_date) end)) as month_name,
  u.full_name AS opportunity_owner,
  verticals.vertical,
  u.email,
  o.business_type,
  a.type as account_type,
  r.name AS record_type,
  datetime_add(oh.created_at_utc, interval 5 hour) as opportunity_close_time
from `fulfillment-dwh-production.pandata_curated.sf_opportunities` o
inner join `fulfillment-dwh-production.pandata_curated.sf_accounts` a
        on o.sf_account_id = a.id and o.global_entity_id = a.global_entity_id 
left  join `fulfillment-dwh-production.pandata_curated.pd_vendors` v
        on v.global_vendor_id = a.global_vendor_id and v.global_entity_id = a.global_entity_id
inner join `dhh---analytics-apac.pandata_pk.pk_accurate_verticals` verticals
        on verticals.grid = a.global_vendor_id
inner join `fulfillment-dwh-production.pandata_curated.sf_record_types` r
        on r.id = o.sf_record_type_id 
left  join `fulfillment-dwh-production.pandata_curated.sf_users` u
        on u.id = o.sf_owner_id  and u.global_entity_id = o.global_entity_id
left join `fulfillment-dwh-production.pandata_curated.sf_opportunity_field_history` oh
       on o.id = oh.sf_opportunity_id 
      and lower(oh.new_value) = 'closed won' 
      and lower(oh.field) = 'stagename'
where a.global_entity_id = 'FP_PK'
  and o.close_date between '2021-06-01' and '2021-06-30'
  and lower(o.business_type) IN ('new business', 'new bussiness', 'winback', 'win back')
  and lower(o.stage_name) in ('closed won', 'closedwon')
  and lower(a.type) IN ('branch - main', 'branch - virtual restaurant')
  and o.is_marked_for_testing_training = False
  and verticals.vertical in ('restaurants', 'shared kitchens', 'concepts', 'home chefs')
  and r.name = 'FP Opportunity'),

first_case as (
  select c.id, c.sf_opportunity_id, datetime_add(c.closed_at_utc, interval 5 hour) as case_close_time, c.closed_date,
  from `fulfillment-dwh-production.pandata_curated.sf_cases` c
  where c.global_entity_id = 'FP_PK' 
    and c.type = 'Quality Check'
    and c.sf_opportunity_id in (select opportunity_id from activations)
    and c.subject in ('Quality Check Win Back', 'Quality Check New Business', 'Quality New Business')
    qualify rank() over(partition by c.sf_opportunity_id order by c.closed_at_utc asc) = 1 
)
select a.*, c.case_close_time, datetime_diff(a.opportunity_close_time, c.case_close_time, day) as onboarding_duration_in_days
from activations as a 
left join first_case as c 
       on a.opportunity_id = c.sf_opportunity_id