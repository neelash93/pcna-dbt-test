{% macro process_constraints(schema_name=None) %}
    {# Fetch constraints for the given schema #}
    {% set constraints = my_new_project.fetch_pk_uk_constraints(schema_name) %}
    {{ print("Printing below") }}
    {{ print(constraints) }}
    {{ constraints.print_table() }}
        {# Loop through the constraints and handle each one #}
    {% for constraint in constraints %}
        {% if constraint.CONSTRAINT_TYPE == 'PRIMARY' %}
            {% set failed_records = adapter.dispatch('test_primary_key', 'dbt_constraints')(constraint.table_name, constraint.column_name, quote_columns=false) %}
        -- {% elif constraint.constraint_type == 'FOREIGN KEY' %}
        --     {% set failed_records = generate_fk_integrity_test(constraint.table_name, constraint.column_name) %}
        {% elif constraint.constraint_type == 'UNIQUE' %}
             {% set failed_records = test('primary_key',model=ref(constraint.table_name),column_names=constraint.column_name) %}
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