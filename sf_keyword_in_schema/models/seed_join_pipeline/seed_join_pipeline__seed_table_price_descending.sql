{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "QA_SCHEMA"
  })
}}

WITH seed_table_prices AS (

  {#Imports seed price data into the workflow to initialize pricing analytics.#}
  SELECT * 
  
  FROM {{ ref('seed_table_prices')}}

),

seed_catalogues AS (

  SELECT * 
  
  FROM {{ ref('seed_catalogues')}}

),

join_seeds AS (

  SELECT 
    in0."ID" AS "ID",
    in0."TABLE" AS "TABLE",
    in0."TABLEPRICE" AS "TABLEPRICE",
    in1."CATALOGUE" AS "CATALOGUE"
  
  FROM seed_table_prices AS in0
  INNER JOIN seed_catalogues AS in1
     ON in0."ID" = in1."ID"

),

seed_table_price_descending AS (

  {#Sorts seed data by price from highest to lowest to highlight premium offerings.#}
  SELECT * 
  
  FROM join_seeds AS in0
  
  ORDER BY TABLEPRICE DESC NULLS LAST

)

SELECT *

FROM seed_table_price_descending
