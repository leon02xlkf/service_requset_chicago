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
        cast(created_date as Timestamps) as created_date,
        cast(last_modified_date as Timestamps) as last_modified_date,
        cast(closed_date as Timestamps) as closed_date,

        cast(created_hour as integer) as created_hour, -- 24 hour
        cast(created_day_of_week as integer) as created_day_of_week, -- 1 to 7
        cast(created_month as integer) as created_month, -- 1 to 12

        -- Geospatial Information
        cast(street_address as string) as street_address,
        cast(zip_code as integer) as integer,
        cast(latitude as float) as float,
        cast(longitude as float) as float,

        -- Aggregation by Week
        date_trunk(date(cast(created_date as Timestamps)), week(sunday)) as created_week

        -- Processing Hours
        case
            when closed_date is null then null
            else timestamp_diff(
                cast(closed_date as Timestamps),
                cast(created_date as Timestamps),
                hour
            )
        end as processing_hours

    from source
)

select * from renamed