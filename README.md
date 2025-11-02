# Kiket E-commerce Lifecycle Template

This template packages customer-lifecycle workflows, marketing dashboards, and dbt models tailored for subscription and replenishment commerce teams. Projects that sync this definition gain:

- A lifecycle workflow (`ecommerce_customer_lifecycle`) that tracks prospects through repeat, churn-risk, and win-back states.
- Board and issue templates for campaign coordination and retention experiments.
- dbt assets that surface contact segmentation and engagement metrics scoped per tenant.
- Dashboards that visualise customer momentum (activations, repeat rate, churn alerts) directly in the Analytics hub.

> **Important:** Issues in this workflow represent **cohorts or lifecycle initiatives**, not individual customers. Millions of contacts remain in your data layer (dbt models, dashboards, automation tools). Use issues to coordinate campaigns (“UK churn watch Q4 experiment”), approvals, and strategy work.

Set Up Checklist:

- Pair this definition repo with a customer data ingest pipeline (Make.com, CDP, or warehouse jobs) that writes `marketing_contacts` and `marketing_events` tables. The bundled dbt models expect those tables and enforce tenancy via the standard macros.
- Because the `ecommerce.customer_data` module is **project scoped**, install it on each lifecycle project that needs access to enriched contacts. Records automatically include `organization_id` and `project_id` so automation hooks, dashboards, and rate limits stay aligned with project membership.
- Enable the Make.com extension (upcoming) or build your own webhook integration so lifecycle transitions can trigger high-volume automations without creating per-customer tickets.
- Run the analytics pipeline (`bundle exec rake analytics:dbt:run`) to materialize lifecycle dashboards before inviting stakeholders to the Analytics hub.
