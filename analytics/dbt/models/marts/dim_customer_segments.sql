with contacts as (
  select
    organization_id,
    contact_id,
    email,
    lifecycle_stage,
    segment_tags,
    coalesce(last_order_date, subscribed_at) as last_engagement_at,
    total_orders,
    total_revenue,
    lifetime_value,
    date_trunc('month', coalesce(subscribed_at, now())) as cohort_month,
    is_current
  from {{ ref('stg_marketing_contacts') }}
  where is_current
),
segments as (
  select
    organization_id,
    contact_id,
    lifecycle_stage,
    unnest(string_to_array(coalesce(segment_tags, ''), ',')) as tag,
    last_engagement_at,
    total_orders,
    total_revenue,
    lifetime_value,
    cohort_month
  from contacts
)

select
  organization_id,
  contact_id,
  lifecycle_stage,
  nullif(trim(tag), '') as segment_name,
  last_engagement_at,
  total_orders,
  total_revenue,
  lifetime_value,
  cohort_month,
  case
    when total_orders >= 2 then 1
    else 0
  end as is_repeat,
  case
    when total_orders = 0 then 0
    else total_revenue / nullif(total_orders, 0)
  end as avg_order_value,
  case
    when lifetime_value > 0 then lifetime_value
    else total_revenue
  end as clv,
  case
    when last_engagement_at is null then 999
    else extract(day from (current_timestamp - last_engagement_at))
  end as days_since_last_engagement
from segments
