{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Unique_174 AS (

  SELECT * 
  
  FROM `` AS in0

),

Unique_172 AS (

  SELECT * 
  
  FROM `` AS in0

),

Join_173_inner AS (

  SELECT 
    in0.product_name AS product_name,
    in0.country_name AS country_name,
    in0.country_abbreviation AS country_abbreviation,
    in0.sub_country_id AS sub_country_id,
    in1.sub_country_id AS Right_sub_country_id
  
  FROM Unique_172 AS in0
  INNER JOIN Unique_174 AS in1
     ON (in0.sub_country_id = in1.sub_country_id)

)

SELECT *

FROM Join_173_inner
