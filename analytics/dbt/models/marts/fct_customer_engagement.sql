with events as (
  select
    organization_id,
    contact_id,
    event_type,
    occurred_at::timestamp as occurred_at,
    orders_count,
    order_value
  from {{ ref('stg_marketing_events') }}
),
weekly_windows as (
  select
    organization_id,
    date_trunc('week', occurred_at) as window_start,
    date_trunc('week', occurred_at) + interval '7 day' as window_end,
    sum(case when event_type = 'order_completed' then 1 else 0 end) as orders_placed,
    sum(order_value) as revenue,
    count(*) filter (where event_type = 'email_open') as email_opens,
    count(*) filter (where event_type = 'workflow_step') as automation_touches,
    count(distinct contact_id) filter (where event_type = 'order_completed') as customers_ordered,
    count(distinct contact_id) filter (where event_type = 'lifecycle_entered_churn_watch') as churn_watch_contacts,
    count(distinct contact_id) filter (where event_type = 'workflow_win_back_converted') as win_back_conversions
  from events
  group by 1, 2, 3
),
counters as (
  select
    ww.organization_id,
    ww.window_start,
    ww.window_end,
    ww.orders_placed,
    ww.revenue,
    ww.email_opens,
    ww.automation_touches,
    ww.customers_ordered,
    ww.churn_watch_contacts,
    ww.win_back_conversions,
    wc.contact_count as total_contacts,
    wc.onboarding_conversions,
    wc.automation_population
  from weekly_windows ww
  left join (
    select
      organization_id,
      count(*) filter (where lifecycle_stage = 'onboarding') as onboarding_contacts,
      count(*) filter (where lifecycle_stage = 'onboarding' and total_orders > 0) as onboarding_conversions,
      count(*) filter (where lifecycle_stage in ('onboarding','engaged_repeat')) as automation_population,
      count(*) as contact_count
    from {{ ref('dim_customer_segments') }}
    group by organization_id
  ) wc using (organization_id)
)

select
  organization_id,
  window_start,
  window_end,
  'onboarding_conversion_rate' as metric,
  case
    when onboarding_contacts = 0 then 0
    else onboarding_conversions::numeric / onboarding_contacts
  end as value
from counters
union all
select
  organization_id,
  window_start,
  window_end,
  'churn_watch_contacts' as metric,
  churn_watch_contacts::numeric as value
from counters
union all
select
  organization_id,
  window_start,
  window_end,
  'win_back_conversions' as metric,
  win_back_conversions::numeric as value
from counters
union all
select
  organization_id,
  window_start,
  window_end,
  'automated_touch_rate' as metric,
  case
    when automation_population = 0 then 0
    else automation_touches::numeric / automation_population
  end as value
from counters
union all
select
  organization_id,
  window_start,
  window_end,
  'churn_watch_rate_delta' as metric,
  churn_watch_contacts::numeric / nullif(total_contacts, 0)
from counters
