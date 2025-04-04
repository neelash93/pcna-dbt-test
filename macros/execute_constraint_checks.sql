{% macro process_constraints(schema_name) %}
    {# Fetch constraints for the given schema #}
    {% set constraints = fetch_constraints(schema_name) %}
    
    {# SQL to create the results table if it doesn't already exist #}
    {{
        dbt_utils.create_table_as(
            "constraint_test_results",
            sql="
                CREATE TABLE IF NOT EXISTS {{ schema_name }}.constraint_test_results (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT,
                    column_name TEXT,
                    constraint_type TEXT,
                    failed_record JSONB
                )
            "
        )
    }}

    {# Loop through the constraints and handle each one #}
    {% for constraint in constraints %}
        {% if constraint.constraint_type == 'PRIMARY KEY' %}
            {% set failed_records = {{primary_key(constraint.table_name, constraint.column_name)}} %}
        -- {% elif constraint.constraint_type == 'FOREIGN KEY' %}
        --     {% set failed_records = generate_fk_integrity_test(constraint.table_name, constraint.column_name) %}
        -- {% elif constraint.constraint_type == 'CHECK' %}
        --     {% set failed_records = generate_check_constraint_test(constraint.table_name, constraint.column_name, constraint.constraint_name) %}
        {% endif %}

        {# Insert failed records into the table #}
        {% for record in failed_records %}
            {{
                dbt_utils.insert_as(
                    "constraint_test_results",
                    columns=["table_name", "column_name", "constraint_type", "failed_record"],
                    values=[
                        constraint.table_name,
                        constraint.column_name,
                        constraint.constraint_type,
                        record
                    ]
                )
            }}
        {% endfor %}
    {% endfor %}
{% endmacro %}


{% macro fetch_constraints(schema_name=None) %}
    {% set schema_condition = '' %}
    {% if schema_name %}
        {% set schema_condition = "AND table_schema = '" ~ schema_name ~ "'" %}
    {% endif %}

    -- Query the INFORMATION_SCHEMA for all constraints
    with constraints as (
        select 
            constraint_name,
            table_name,
            constraint_type,
            column_name
        from 
            information_schema.table_constraints tc
            join information_schema.key_column_usage kcu
                on tc.constraint_name = kcu.constraint_name
        where 
            tc.constraint_type in ('PRIMARY KEY', 'FOREIGN KEY', 'CHECK')
            {{ schema_condition }}
    )
    select * from constraints
{% endmacro %}