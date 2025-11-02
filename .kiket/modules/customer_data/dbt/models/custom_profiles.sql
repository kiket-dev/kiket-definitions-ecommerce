select
  *
from {{ source('custom_data', 'customer_profiles') }}
