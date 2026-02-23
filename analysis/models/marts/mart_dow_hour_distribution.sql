select
  created_day_of_week,
  created_hour,
  count(*) as request_cnt
from {{ ref('stg_srchicago_311_raw') }}
group by 1, 2