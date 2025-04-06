{% macro fetch_pk_uk_constraints(schema_name=None) %}
    {% set schema_condition = '' %}
    {% if schema_name %}
        {% set schema_condition = "AND table_schema = '" ~ schema_name ~ "'" %}
    {% endif %}
    
    {%- set query_tb -%}
        CREATE OR REPLACE TABLE PRIM6 (
            CONSTRAINT_TYPE VARCHAR(20),
            CREATED_ON TIMESTAMP_NTZ,
            DATABASE_NAME VARCHAR(30),
            SCHEMA_NAME VARCHAR(30),
            TABLE_NAME VARCHAR(50),
            COLUMN_NAME VARCHAR(50),
            KEY_SEQUENCE INT,
            CONSTRAINT_NAME VARCHAR(100),
            RELY VARCHAR(10),
            COMMENTS VARCHAR(512)
        )
    {%- endset -%}

    {%- set query_pk -%}
        show primary keys
    {%- endset -%}
    {%- set query_uk -%}
        show unique keys
    {%- endset -%}
    {%- set query_ins_pk -%}
        INSERT INTO PRIM6 SELECT 'PRIMARY', * FROM TABLE(RESULT_SCAN(-1))
    {%- endset -%}
    {%- set query_ins_uk -%}
        INSERT INTO PRIM6 SELECT 'UNIQUE', * FROM TABLE(RESULT_SCAN(-1))
    {%- endset -%}
    
    {%- set query_final -%}
        SELECT SCHEMA_NAME, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, LISTAGG(COLUMN_NAME, ',') AS COLUMN_NAMES 
FROM  PRIM6 GROUP BY SCHEMA_NAME, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE ;
    {%- endset -%}


    {% do run_query(query_tb) %}
    {% do run_query(query_pk) %}    
    {% do run_query(query_ins_pk) %}
    {% do run_query(query_uk) %}    
    {% do run_query(query_ins_uk) %}
    {% set result_final = run_query(query_final) %}

    {% for constraint in result_final %}
        {{ print(constraint.SCHEMA_NAME + "   " + constraint.TABLE_NAME + "  " + constraint.COLUMN_NAMES + "  " + constraint.CONSTRAINT_TYPE) }}
        {% set col_arr = constraint.COLUMN_NAMES.split(',') %}
        {{ print(col_arr) }}
        {% if constraint.CONSTRAINT_TYPE == 'PRIMARY' %}
            {% set pk_query = adapter.dispatch('test_primary_key', 'dbt_constraints')( constraint.TABLE_NAME, col_arr, quote_columns=true) %}
            {{ print(pk_query) }}
            {% set failed_records = run_query(pk_query) %}
            {{ print(failed_records|length) }}
            {% for row in failed_records %}
                {{ print(row) }}
            {% endfor %}
        {% endif %}
    {% endfor %}   
{% endmacro %}