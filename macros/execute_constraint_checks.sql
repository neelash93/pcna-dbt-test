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
            {% set query = adapter.dispatch('test_primary_key', 'dbt_constraints')( constraint.TABLE_NAME, col_arr, quote_columns=true) %}
            {{ print(query) }}
        {% elif constraint.CONSTRAINT_TYPE == 'UNIQUE' %}
            {% set query = adapter.dispatch('test_unique_key', 'dbt_constraints')( constraint.TABLE_NAME, col_arr, quote_columns=true) %}
            {{ print(query) }}
        {% endif %}
       
        {% set failed_records = run_query(query) %}
        {% set status = "" %}
        {% if failed_records | length > 0 %}
            {% set status = "FAIL" %}
        {% else %}
            {% set status = "PASS" %}
        {% endif %}
        {% set insrt_query = "INSERT INTO CONSTRAINT_TEST_RESULTS (table_name, column_name, constraint_type, status, run_id) values ('" + constraint.TABLE_NAME + "', '" + constraint.COLUMN_NAMES + "', '" + constraint.CONSTRAINT_TYPE + "', '" + status + "', '" + invocation_id + "')" %}
        {% do run_query(insrt_query) %}
        {% for row in failed_records %}
            {% set fail = row.values() | join(', ') %}
            {{ print("This is fail : " + fail) }}
            {% set insrt_query_fail = "INSERT INTO CONSTRAINT_TEST_RESULTS_FAILED_RECORDS (table_name, column_name, constraint_type, failed_record, run_id) values ('" + constraint.TABLE_NAME + "', '" + constraint.COLUMN_NAMES + "', '" + constraint.CONSTRAINT_TYPE + "', '" + fail + "', '" + invocation_id + "')" %}
            {% do run_query(insrt_query_fail) %}
        {% endfor %}
        
    {% endfor %}   
{% endmacro %}