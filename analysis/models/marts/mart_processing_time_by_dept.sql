select
  owner_department,
  count(*) as total_requests,
  countif(processing_hours is not null) as closed_requests,

  avg(processing_hours) as avg_processing_hours,
  approx_quantiles(processing_hours, 100)[offset(50)] as p50_processing_hours,
  approx_quantiles(processing_hours, 100)[offset(90)] as p90_processing_hours
  
from {{ ref('stg_srchicago_311_raw') }}
where owner_department is not null
group by 1