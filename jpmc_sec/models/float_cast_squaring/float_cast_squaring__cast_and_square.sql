{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH float_sample_data AS (

  SELECT * 
  
  FROM {{ ref('float_sample_data')}}

),

cast_double AS (

  SELECT 
    id,
    CAST(value_a AS DOUBLE) AS value_a_double,
    CAST(value_b AS DOUBLE) AS value_b_double,
    CAST(value_c AS DOUBLE) AS value_c_double
  
  FROM seed_source

),

cast_decimal_19_6 AS (

  {#Standardizes numeric values to a consistent format so financial calculations and reports use reliable, comparable figures.#}
  SELECT 
    id,
    CAST(value_a AS DECIMAL (10, 10)) AS value_a_dec_19_6,
    CAST(value_b AS DECIMAL (19, 6)) AS value_b_dec_19_6,
    CAST(value_c AS DECIMAL (19, 6)) AS value_c_dec_19_6
  
  FROM seed_source

),

cast_decimal_19_0 AS (

  SELECT 
    id,
    CAST(value_a AS DECIMAL (19, 0)) AS value_a_dec_19_0,
    CAST(value_b AS DECIMAL (19, 0)) AS value_b_dec_19_0,
    CAST(value_c AS DECIMAL (19, 0)) AS value_c_dec_19_0
  
  FROM seed_source

),

join_all_casts AS (

  SELECT 
    cast_decimal_19_6.id,
    cast_decimal_19_6.value_a_dec_19_6,
    cast_decimal_19_6.value_b_dec_19_6,
    cast_decimal_19_6.value_c_dec_19_6,
    cast_decimal_19_0.value_a_dec_19_0,
    cast_decimal_19_0.value_b_dec_19_0,
    cast_decimal_19_0.value_c_dec_19_0,
    cast_double.value_a_double,
    cast_double.value_b_double,
    cast_double.value_c_double
  
  FROM cast_decimal_19_6
  INNER JOIN cast_decimal_19_0
     ON cast_decimal_19_6.id = cast_decimal_19_0.id
  INNER JOIN cast_double
     ON cast_decimal_19_6.id = cast_double.id

),

cast_and_square AS (

  SELECT 
    id,
    value_a_dec_19_6,
    value_a_dec_19_6 * value_a_dec_19_6 AS value_a_dec_19_6_squared,
    value_b_dec_19_6,
    value_b_dec_19_6 * value_b_dec_19_6 AS value_b_dec_19_6_squared,
    value_c_dec_19_6,
    value_c_dec_19_6 * value_c_dec_19_6 AS value_c_dec_19_6_squared,
    value_a_dec_19_0,
    value_a_dec_19_0 * value_a_dec_19_0 AS value_a_dec_19_0_squared,
    value_b_dec_19_0,
    value_b_dec_19_0 * value_b_dec_19_0 AS value_b_dec_19_0_squared,
    value_c_dec_19_0,
    value_c_dec_19_0 * value_c_dec_19_0 AS value_c_dec_19_0_squared,
    value_a_double,
    value_a_double * value_a_double AS value_a_double_squared,
    value_b_double,
    value_b_double * value_b_double AS value_b_double_squared,
    value_c_double,
    value_c_double * value_c_double AS value_c_double_squared
  
  FROM join_all_casts

)

SELECT *

FROM cast_and_square
