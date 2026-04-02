{{
  config({    
    "materialized": "table",
    "alias": "table_test",
    "database": "avpreet_qa",
    "schema": "avpreettables"
  })
}}

WITH undefined_output_xml AS (

  {#Overwrites the seed data for the sr2 dataset to refresh the source.#}
  SELECT * 
  
  FROM {{ ref('sr2')}}

),

Reformat_1 AS (

  {#Attempts to reformat output by renaming ID to integer and excluding the original ID in a data import scenario.#}
  SELECT 
    CAST(ID AS INT) AS ID,
    * EXCEPT (ID)
  
  FROM undefined_output_xml AS in0

)

{#Overwrites the table test with prepared data from the source table avpreet_qa_avpreettables for downstream analysis.#}
SELECT *

FROM Reformat_1
