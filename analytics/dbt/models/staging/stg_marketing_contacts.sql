with source_contacts as (
  select
    contact_id,
    organization_id,
    email,
    phone,
    country_code,
    lifecycle_stage,
    segment_tags,
    subscribed_at,
    unsubscribed_at,
    last_order_date,
    total_orders,
    total_revenue,
    lifetime_value,
    now()::timestamp as ingested_at
  from {{ source('customer_data', 'marketing_contacts') }}
  where {{ tenant_filter('organization_id') }}
)

select
  contact_id,
  organization_id,
  lower(email) as email,
  phone,
  country_code,
  lifecycle_stage,
  segment_tags,
  subscribed_at,
  unsubscribed_at,
  last_order_date,
  total_orders,
  total_revenue,
  lifetime_value,
  row_number() over (partition by organization_id, contact_id order by ingested_at desc) = 1 as is_current
from source_contacts
