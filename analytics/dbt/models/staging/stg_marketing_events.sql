select
  event_id,
  organization_id,
  contact_id,
  event_type,
  channel,
  campaign_id,
  occurred_at,
  metadata,
  case
    when event_type = 'order_completed' then 1
    else 0
  end as orders_count,
  case
    when event_type = 'order_completed' then (metadata->>'order_total')::numeric
    else 0
  end as order_value
from {{ source('customer_data', 'marketing_events') }}
where {{ tenant_filter('organization_id') }}
