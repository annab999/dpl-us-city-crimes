{%- for cityname in var('cities') %}
{{ "with " if loop.first }}{{cityname}}_all as (
    select
        id,
        time_stamp,
        city,
        victim_age
    from {{ ref('stg_crimes_' ~ cityname) }}
),
{%- endfor %}
cities_unioned as (
{%- for cityname in var('cities') %}
    select * from {{cityname}}_all
    {{ "union all" if not loop.last }}
{%- endfor %}
)
    select  
        city,
        datetime_trunc(time_stamp, month) as mon,
        count(id) as records,
        min(victim_age) as youngest_victim
    from cities_unioned
    group by 1, 2

-- dbt build -m <model.sql> --var 'is_test_run: false'
{%- if var('is_test_run', default=true) %}       
    limit 100
{% endif %}