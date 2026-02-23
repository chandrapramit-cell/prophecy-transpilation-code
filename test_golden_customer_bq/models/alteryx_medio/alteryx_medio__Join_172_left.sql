{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH aka_Server_UYDB_220 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_220_ref') }}

),

Union_170 AS (

  SELECT *
  
  FROM {{ ref('alteryx_medio__Union_170')}}

),

Join_172_left AS (

  SELECT in0.*
  
  FROM Union_170 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.descripcion_encuesta_sistema
    
    FROM aka_Server_UYDB_220 AS in1
    
    WHERE in1.descripcion_encuesta_sistema IS NOT NULL
  ) AS in1_keys
     ON (in0.descripcion_encuesta = in1_keys.descripcion_encuesta_sistema)
  
  WHERE (in1_keys.descripcion_encuesta_sistema IS NULL)

)

SELECT *

FROM Join_172_left
