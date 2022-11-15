/*
    specific macros for use on different cities
*/

-- Chicago

{% macro arrest(col) %}

    case {{ col }}
        when true then "arrested"
        when false then "no arrest"
    end

{% endmacro %}
