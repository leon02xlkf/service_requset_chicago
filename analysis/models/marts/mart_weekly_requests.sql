with base as (
  select created_week
  from {{ ref('stg_srchicago_311_raw') }}
),

weekly as (
  select
    created_week,
    count(*) as weekly_requests
  from base
  group by 1
)

select
    created_week,
    weekly_requests
from weekly
order by created_week

-- Moving Average

-- select
--   created_week,
--   weekly_requests,
--   avg(weekly_requests) over (
--     order by created_week
--     rows between 3 preceding and current row
--   ) as ma_4w,
--   lag(weekly_requests) over (order by created_week) as prev_week_requests,
--   safe_divide(
--     weekly_requests - lag(weekly_requests) over (order by created_week),
--     lag(weekly_requests) over (order by created_week)
--   ) as wow_growth_rate
-- from weekly