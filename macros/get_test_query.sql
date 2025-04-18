{% macro get_test_query(constraint_type, table_name, column_name_csv, pk_table_name=None, pk_column_name_csv=None) %}
    {% if constraint_type == 'PRIMARY' %}
        {{ get_primary_key_query(table_name, column_name_csv) }}
    {% elif constraint_type == 'UNIQUE' %}
        {{ get_unique_key_query(table_name, column_name_csv) }}
    {% elif constraint_type == 'FOREIGN' %}
        {{ get_foreign_key_query(table_name, column_name_csv, pk_table_name, pk_column_name_csv) }}
    {% else %}
        SELECT NULL;
    {% endif %}
{% endmacro %}

{% macro get_primary_key_query(table_name, columns_csv) %}
    
    {%- set column_arr = columns_csv.split(",") %}
    {%- set columns_concat = column_arr | join("||', '||") %}

    with validation_errors as (
    select
        {{columns_csv}}, count(*) as row_count
    from {{table_name}}
    group by {{columns_csv}}
    having count(*) > 1
        {% for column in column_arr -%}
        or {{column}} is null
        {% endfor %}
    )
    select 'There are '||row_count||' occurrences of ('||{{columns_concat}}||') in the field ({{columns_csv}})' as FAIL_MSG
    from validation_errors
{% endmacro %}



{% macro get_unique_key_query(table_name, columns_csv) %}
    
    {%- set columns_arr = columns_csv.split(",") %}
    {%- set columns_concat = columns_arr | join("||', '||") %}

    with validation_errors as (
    select
        {{columns_csv}}, count(*) as row_count
    from {{table_name}}
    group by {{columns_csv}}
    having count(*) > 1
    )
    select 'There are '||row_count||' occurrences of ('||{{columns_concat}}||') in the field ({{columns_csv}})' as FAIL_MSG
    from validation_errors
{% endmacro %}


{% macro get_foreign_key_query(fk_table_name, fk_columns_csv, pk_table_name, pk_columns_csv) %}
    
    {%- set fk_columns_arr = fk_columns_csv.split(",") %}
    {%- set fk_columns_concat = fk_columns_arr | join("||', '||") %}
    {%- set pk_columns_arr = pk_columns_csv.split(",") %}
    {%- set join_conditions = [] -%}
    {%- for x in range(fk_columns_arr|count) -%}
        {%- set join_conditions = join_conditions.append( 'parent.' ~ pk_columns_arr[x] ~ ' = child.' ~ fk_columns_arr[x] ) -%}
    {%- endfor -%}

    {#- This test will return if all the columns are not null
        and the values are not found in the referenced PK table #}
    with child as (
    select
        {{fk_columns_csv}}, count(*) as row_count
    from {{fk_table_name}}
    where 1=1
            {% for column in fk_columns_arr -%}
            and {{column}} is not null
            {% endfor %}
    group by {{fk_columns_csv}}
    ),
    parent as (
        select
            {{pk_columns_csv}}
        from {{pk_table_name}}
    ),
    validation_errors as (
        select
            child.*
        from child
        left join parent
            on {{join_conditions | join(' and ')}}
        where parent.{{pk_columns_arr | first}} is null
    )
    select 'There are '||row_count||' occurrences of ('||{{fk_columns_concat}}||') in fields ({{fk_columns_csv}}) which are not found in the primary table fields ({{pk_columns_csv}})' as FAIL_MSG
    from validation_errors
{% endmacro %}