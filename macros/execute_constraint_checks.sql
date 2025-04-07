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

    {%- set query_tb_fk -%}
        CREATE OR REPLACE TABLE PRIM6_fk (
            CONSTRAINT_TYPE VARCHAR(20),
            CREATED_ON TIMESTAMP_NTZ,
            PK_DATABASE_NAME VARCHAR(30),
            PK_SCHEMA_NAME VARCHAR(30),
            PK_TABLE_NAME VARCHAR(50),
            PK_COLUMN_NAME VARCHAR(50),
            FK_DATABASE_NAME VARCHAR(30),
            FK_SCHEMA_NAME VARCHAR(30),
            FK_TABLE_NAME VARCHAR(50),
            FK_COLUMN_NAME VARCHAR(50),
            KEY_SEQUENCE INT,
            UPDATE_RULE VARCHAR(50),
            DELETE_RULE VARCHAR(50),
            FK_NAME VARCHAR(100),
            PK_NAME VARCHAR(100),
            DEFERRABILITY VARCHAR(100),
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
    {%- set query_fk -%}
        show imported keys
    {%- endset -%}
    {%- set query_ins_pk -%}
        INSERT INTO PRIM6 SELECT 'PRIMARY', * FROM TABLE(RESULT_SCAN(-1))
    {%- endset -%}
    {%- set query_ins_uk -%}
        INSERT INTO PRIM6 SELECT 'UNIQUE', * FROM TABLE(RESULT_SCAN(-1))
    {%- endset -%}
    {%- set query_ins_fk -%}
        INSERT INTO PRIM6_fk SELECT 'FOREIGN', * FROM TABLE(RESULT_SCAN(-1))
    {%- endset -%}
    
    {%- set query_final -%}
        SELECT SCHEMA_NAME, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, NULL AS PK_TABLE_NAME, LISTAGG(COLUMN_NAME, ',') AS COLUMN_NAMES, NULL AS PK_COLUMN_NAMES
FROM  PRIM6 GROUP BY SCHEMA_NAME, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE 
UNION
SELECT FK_SCHEMA_NAME, FK_TABLE_NAME, FK_NAME, CONSTRAINT_TYPE, PK_TABLE_NAME, LISTAGG(FK_COLUMN_NAME, ',') AS COLUMN_NAMES, LISTAGG(PK_COLUMN_NAME, ',') AS PK_COLUMN_NAMES
FROM PRIM6_FK GROUP BY FK_SCHEMA_NAME, FK_TABLE_NAME, FK_NAME, CONSTRAINT_TYPE, PK_TABLE_NAME;
    {%- endset -%}


    {% do run_query(query_tb) %}
    {% do run_query(query_tb_fk) %}
    {% do run_query(query_pk) %}    
    {% do run_query(query_ins_pk) %}
    {% do run_query(query_uk) %}    
    {% do run_query(query_ins_uk) %}
    {% do run_query(query_fk) %}    
    {% do run_query(query_ins_fk) %}
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
        {% elif constraint.CONSTRAINT_TYPE == 'FOREIGN' %}
            {% set pk_col_arr = constraint.PK_COLUMN_NAMES.split(',') %}
            {{ print(col_arr) }}
            {% set query = adapter.dispatch('test_foreign_key', 'dbt_constraints')( constraint.TABLE_NAME, col_arr, constraint.PK_TABLE_NAME, pk_col_arr, quote_columns=true) %}
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