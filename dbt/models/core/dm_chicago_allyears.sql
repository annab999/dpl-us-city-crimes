with chicago_data as (
    select *
    from {{ ref('stg_crimes_chicago') }}
)
    select
        id,
        time_stamp,
        street,
        area,
        type,
        description,
        status
    from chicago_data

-- dbt build -m <model.sql> --var 'is_test_run: false'
{%- if var('is_test_run', default=true) %}       
    limit 100
{% endif %}
