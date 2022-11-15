/*
    macros for use on different cities
*/

{% macro city_ctes(cityname) %}

{%- for yr in var('years_' ~ cityname) %}
{{ "with " if loop.first }}{{ cityname }}_{{yr}} as (
    select *
    from {{ source('part_per_year', cityname ~ '_' ~ yr ~ '_part')  }}
    where id is not null
),
{% endfor %}
{{cityname}}_years_unioned as (
{%- for yr in var('years_' ~ cityname) %}
    select * from {{cityname}}_{{yr}}
    {{ "union all" if not loop.last }}
{%- endfor %}
)
    select
        cast(id as string) as id,
        cast(timestamp as datetime) as time_stamp,
        city,
        street,
        lower(type) as type,
        lower(description) as description,
        status,
        cast(area as string) as area,
        cast(victim_age as integer) as victim_age
    from {{cityname}}_years_unioned

{% endmacro %}