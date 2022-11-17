with chicago_data as (
    select *
    from {{ ref('stg_crimes_chicago') }}
)
    select
        id,
        time_stamp,
        extract(year from time_stamp) as yr,
        extract(month from time_stamp) as mon,
        date_trunc(time_stamp, month) as trunc_to_mon,
        street,
        area,
        type,
        description,
        {{ arrest('status') }} as arrest_status
    from chicago_data

-- dbt build -m <model.sql> --var 'is_test_run: false'
{%- if var('is_test_run', default=true) %}       
    limit 100
{% endif %}
