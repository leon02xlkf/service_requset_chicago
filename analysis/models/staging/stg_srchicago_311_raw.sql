with source as (
    select * from {{ source('raw_data', 'srchicago_311_v2') }}
),

renamed as (
    select
        -- Basic identities
        cast(sr_number as string) as sr_number,
        cast(sr_type as string) as sr_type,
        cast(owner_department as string) as owner_department,
        cast(status as string) as status,
        cast(origin as string) as origin,
        
        -- Timestamps
        -- Format looks like: 2024-10-23 09:23:06 UTC
        safe_cast(created_date as timestamp) as created_date,
        safe_cast(last_modified_date as timestamp) as last_modified_date,
        safe_cast(closed_date as timestamp) as closed_date,

        safe_cast(created_hour as int64) as created_hour, -- 24 hour
        safe_cast(created_day_of_week as int64) as created_day_of_week, -- 1 to 7
        safe_cast(created_month as int64) as created_month, -- 1 to 12

        -- Geospatial Information
        cast(street_address as string) as street_address,
        safe_cast(zip_code as int64) as zip_code,
        safe_cast(latitude as float64) as latitude,
        safe_cast(longitude as float64) as longitude,

        -- Aggregation by Week
        date_trunc(date(safe_cast(created_date as timestamp)), week(sunday)) as created_week,

        -- Processing Hours
        case
            when closed_date is null then null
            else timestamp_diff(
                safe_cast(closed_date as timestamp),
                safe_cast(created_date as timestamp),
                hour
            )
        end as processing_hours

    from source
)

select * from renamed