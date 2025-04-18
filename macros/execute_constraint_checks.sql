{% macro execute_constraint_checks(database_name=None, schema_name=None, table_name=None, purge=None) %}
    {% set database_condition = '' %}
    {% set schema_condition = '' %}
    {% set table_condition = '' %}
    
    -- Define conditions if provided as argument
    {% if database_name %}
        {% set database_condition = "AND DATABASE_NAME = '" ~ database_name ~ "'" %}
    {% endif %}
    {% if schema_name %}
        {% set schema_condition = "AND SCHEMA_NAME = '" ~ schema_name ~ "'" %}
    {% endif %}
    {% if table_name %}
        {% set table_condition = "AND TABLE_NAME = '" ~ table_name ~ "'" %}
    {% endif %}
    
    --Queries for storing list of constraints in temp table
    {%- set query_tmp_pkuk -%}
        CREATE OR REPLACE TEMP TABLE temp_pk_uk (
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

    {%- set query_tmp_fk -%}
        CREATE OR REPLACE TABLE temp_fk (
            CONSTRAINT_TYPE VARCHAR(20),
            CREATED_ON TIMESTAMP_NTZ,
            PK_DATABASE_NAME VARCHAR(30),
            PK_SCHEMA_NAME VARCHAR(30),
            PK_TABLE_NAME VARCHAR(50),
            PK_COLUMN_NAME VARCHAR(50),
            DATABASE_NAME VARCHAR(30),
            SCHEMA_NAME VARCHAR(30),
            TABLE_NAME VARCHAR(50),
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
        INSERT INTO temp_pk_uk SELECT 'PRIMARY', * FROM TABLE(RESULT_SCAN(-1))
    {%- endset -%}
    {%- set query_ins_uk -%}
        INSERT INTO temp_pk_uk SELECT 'UNIQUE', * FROM TABLE(RESULT_SCAN(-1))
    {%- endset -%}
    {%- set query_ins_fk -%}
        INSERT INTO temp_fk SELECT 'FOREIGN', * FROM TABLE(RESULT_SCAN(-1))
    {%- endset -%}

    -- Queries to create table storing test results
    {%- set query_tb_summary -%}
        CREATE TABLE IF NOT EXISTS CONSTRAINT_TEST_SUMMARY (
            id INT AUTOINCREMENT,
            database_name VARCHAR(30),
            schema_name VARCHAR(30),
            table_name VARCHAR(80) NOT NULL,
            column_name VARCHAR(30) NOT NULL,
            referred_table_name VARCHAR(80) DEFAULT NULL,
            referred_column_name VARCHAR(30) DEFAULT NULL,
            constraint_type VARCHAR(15),
            Status VARCHAR(20),
            run_id STRING,
            INSRT_DT timestamp_ntz DEFAULT CURRENT_TIMESTAMP
        );
    {%- endset -%}
    {%- set query_tb_detail -%}
        CREATE TABLE IF NOT EXISTS CONSTRAINT_TEST_DETAIL (
            id INT AUTOINCREMENT,
            database_name VARCHAR(30),
            schema_name VARCHAR(30),
            table_name VARCHAR(80) NOT NULL,
            column_name VARCHAR(30) NOT NULL,
            referred_table_name VARCHAR(80) DEFAULT NULL,
            referred_column_name VARCHAR(30) DEFAULT NULL,
            constraint_type VARCHAR(15),
            failed_record VARCHAR(255),
            run_id STRING,
            INSRT_DT timestamp_ntz DEFAULT CURRENT_TIMESTAMP
        );
    {%- endset -%}
    
    --Final query to get final list of all constraints that need to be evaluated
    {%- set query_final -%}
        SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, NULL AS PK_TABLE_NAME, LISTAGG(COLUMN_NAME, ',') AS COLUMN_NAMES, NULL AS PK_COLUMN_NAMES
FROM  temp_pk_uk WHERE 1=1 {{ database_condition }} {{ schema_condition }} {{ table_condition }} GROUP BY DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE
UNION
SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FK_NAME, CONSTRAINT_TYPE, PK_TABLE_NAME, LISTAGG(FK_COLUMN_NAME, ',') AS COLUMN_NAMES, LISTAGG(PK_COLUMN_NAME, ',') AS PK_COLUMN_NAMES
FROM temp_fk WHERE 1=1 {{ database_condition }} {{ schema_condition }} {{ table_condition }} GROUP BY DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FK_NAME, CONSTRAINT_TYPE, PK_TABLE_NAME;
    {%- endset -%}

    -- Run all queries defined above sequentially
    {% do run_query(query_tmp_pkuk) %}
    {% do run_query(query_tmp_fk) %}
    {% do run_query(query_pk) %}    
    {% do run_query(query_ins_pk) %}
    {% do run_query(query_uk) %}    
    {% do run_query(query_ins_uk) %}
    {% do run_query(query_fk) %}    
    {% do run_query(query_ins_fk) %}
    {% do run_query(query_tb_summary) %}    
    {% do run_query(query_tb_detail) %}
    {% set result_final = run_query(query_final) %}

    --For each constraint identified - Call DBT Constraint Tests to generate test query
    {% for constraint in result_final %}
        {{ print(constraint.SCHEMA_NAME + "   " + constraint.TABLE_NAME + "  " + constraint.COLUMN_NAMES + "  " + constraint.CONSTRAINT_TYPE) }}
        
        {% set query = get_test_query(constraint.CONSTRAINT_TYPE, constraint.TABLE_NAME, constraint.COLUMN_NAMES, constraint.PK_TABLE_NAME, constraint.PK_COLUMN_NAMES) %}
        
        --Run test query, and INSERT into Summary and Detail tables
        {% set failed_records = run_query(query) %}
        {% set status = "" %}

        --Set Status to FAIL if record length > 0, else pass
        {% if failed_records | length > 0 %}
            {% set status = "FAIL (" ~ failed_records | length ~ ")" %}
        {% else %}
            {% set status = "PASS" %}
        {% endif %}

        --Insert into SUMMARY table
        {% if constraint.CONSTRAINT_TYPE == 'FOREIGN' %}
            {% set insrt_query = "INSERT INTO CONSTRAINT_TEST_SUMMARY (database_name, schema_name, table_name, column_name, referred_table_name, referred_column_name, constraint_type, status, run_id) values ('" + constraint.DATABASE_NAME + "', '"  + constraint.SCHEMA_NAME + "', '" + constraint.TABLE_NAME + "', '" + constraint.COLUMN_NAMES + "', '" + constraint.PK_TABLE_NAME + "', '" + constraint.PK_COLUMN_NAMES + "', '" + constraint.CONSTRAINT_TYPE + "', '" + status + "', '" + invocation_id + "')" %}
        {% else %}
            {% set insrt_query = "INSERT INTO CONSTRAINT_TEST_SUMMARY (database_name, schema_name, table_name, column_name, constraint_type, status, run_id) values ('" + constraint.DATABASE_NAME + "', '"  + constraint.SCHEMA_NAME + "', '" + constraint.TABLE_NAME + "', '" + constraint.COLUMN_NAMES + "', '" + constraint.CONSTRAINT_TYPE + "', '" + status + "', '" + invocation_id + "')" %}        
        {% endif %}
        {% do run_query(insrt_query) %}

        --Insert each failure record into DETAIL table
        {% for row in failed_records %}
            {% set fail = row.values() | join(', ') %} --Change to meaningful string
            {% if constraint.CONSTRAINT_TYPE == 'FOREIGN' %}
                {% set insrt_query_fail = "INSERT INTO CONSTRAINT_TEST_DETAIL (database_name, schema_name, table_name, column_name, referred_table_name, referred_column_name, constraint_type, failed_record, run_id) values ('" + constraint.DATABASE_NAME + "', '"  + constraint.SCHEMA_NAME + "', '" + constraint.TABLE_NAME + "', '" + constraint.COLUMN_NAMES + "', '" + constraint.PK_TABLE_NAME + "', '" + constraint.PK_COLUMN_NAMES + "', '" + constraint.CONSTRAINT_TYPE + "', '" + fail + "', '" + invocation_id + "')" %}
            {% else %}
                {% set insrt_query_fail = "INSERT INTO CONSTRAINT_TEST_DETAIL (database_name, schema_name, table_name, column_name, constraint_type, failed_record, run_id) values ('" + constraint.DATABASE_NAME + "', '"  + constraint.SCHEMA_NAME + "', '" + constraint.TABLE_NAME + "', '" + constraint.COLUMN_NAMES + "', '" + constraint.CONSTRAINT_TYPE + "', '" + fail + "', '" + invocation_id + "')" %}
            {% endif %}
            {% do run_query(insrt_query_fail) %}
        {% endfor %}
        
    {% endfor %}

    {% if purge is not none and purge | int > 0 %}
        {% do run_query("DELETE FROM CONSTRAINT_TEST_SUMMARY WHERE INSRT_DT < DATEADD(days, -" + purge | string + ", CURRENT_DATE)") %}
        {% do run_query("DELETE FROM CONSTRAINT_TEST_DETAIL WHERE INSRT_DT < DATEADD(days, -" + purge | string + ", CURRENT_DATE)") %}
    {% endif %}

{% endmacro %}