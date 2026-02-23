select
  sr_type,
  count(*) as request_cnt
from {{ ref('stg_srchicago_311_raw') }}
group by 1
qualify row_number() over (order by request_cnt desc) <= 10