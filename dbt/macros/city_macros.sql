/*
    macros for use on different cities
*/

{% macro city_ctes(cityname) %}

{%- for yr in var(years_cityname) %}
{{ "with " if loop.first }}{{cityname}}_{{yr}} as (
    select *
    from {{ source('part_per_year', 'cityname_yr_part')  }}
),
{% endfor %}
{{cityname}}_years_unioned as (
{%- for yr in var(years_cityname) %}
    select * from {{cityname}}_{{yr}}
    {{ "union all" if not loop.last }}
{% endfor %}
)
select
    *
from {{cityname}}_years_unioned

{% endmacro %}