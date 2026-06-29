{{ config(materialized='view') }}

SELECT
    category_sk,
    category_nk,
    category_name,
    category_identifier,
    created_date,
    extracted_date,
    dw_load_date,
    gold_load_date
FROM {{ source('gold_delta', 'gold_dim_productcategory') }}
