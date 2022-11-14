/*
    macros for use on unioned cities
*/

{% macro selector(category) %}

    case {{ category }}
        when general then '*'
        when location then 'timestamp, city, street, area'
        when victim then 'timestamp, city, victim_age'
        end

{% endmacro %}


{% macro combine(category) %}

{%- for cityname in var(cities) %}
{{ "with " if loop.first }}cityname_all as (
    select
        {{ selector(category) }}
    from {{ ref(stg_crimes_cityname) }}
),
{% endfor %}

{% endmacro %}