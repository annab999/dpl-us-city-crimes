{%- for cityname in var('cities') %}
{{ "with " if loop.first }}{{cityname}}_all as (
    select
        id,
        time_stamp,
        city,
        {{ age('victim_age') }} as victim_age
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
        datetime_trunc(time_stamp, month) as trunc_to_mon,
        count(id) as records,
        min(victim_age) as youngest_victim,
        avg(victim_age) as avg_victim_age,
        max(victim_age) as oldest_victim
    from cities_unioned
    group by 1, 2

-- dbt build -m <model.sql> --var 'is_test_run: false'
{%- if var('is_test_run', default=true) %}       
    limit 100
{% endif %}
