{{ config(materialized='table', properties={"format": "'PARQUET'"}) }}

SELECT
    category_sk,
    category_nk,
    category_name,
    category_identifier,
    created_date,
    extracted_date,
    dw_load_date,
    gold_load_date
FROM {{ source('gold_iceberg', 'gold_dim_productcategory') }}
