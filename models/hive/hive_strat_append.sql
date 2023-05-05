{{ config(
  materialized='incremental',
  incremental_strategy='append',
  format='avro'
) }}

SELECT
    ts AS test_timestamp
    , CAST(ts AS DATE) AS test_date
    , 'abc' AS test_string_varchar
    , cast('a' as varchar(1)) as test_fixed_varchar
    , true AS test_bool
    , 123 AS test_int
    , cast(123.45 as real) AS test_float
    , CAST(1234.567 AS DECIMAL(7, 3)) AS test_decimal
    , CAST(34 AS BIGINT) AS test_bigint
    , CAST(123.33 AS DOUBLE) AS test_double
    , ARRAY['a'] AS test_array_string
    , ARRAY[1] AS test_array_int
    , ARRAY[false] AS test_array_bool
    , ARRAY[ARRAY['a']] AS test_array_array_string
FROM {{ ref('hive_data_source') }}
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  WHERE ts > (SELECT max(test_timestamp) FROM {{ this }})
{% endif %}
