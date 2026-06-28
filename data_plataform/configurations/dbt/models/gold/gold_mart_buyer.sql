{{ config(
    materialized='table',
    properties={
        "format": "'PARQUET'"
    }
) }}

SELECT
    businessentityid,
    firstname,
    lastname,
    gender,
    jobtitle,
    hiredate,
    departmentid,
    name          AS department_name,
    groupname     AS department_group,
    rate          AS salary_rate,
    payfrequency,
    salariedflag,
    currentflag,
    extracted_date,
    modified_date
FROM {{ source('gold_iceberg', 'gold_dim_buyer') }}
WHERE currentflag = 1
