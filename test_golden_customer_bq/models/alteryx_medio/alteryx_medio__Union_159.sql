{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_172_left AS (

  SELECT *
  
  FROM {{ ref('alteryx_medio__Join_172_left')}}

),

TextInput_224 AS (

  SELECT * 
  
  FROM {{ ref('seed_224')}}

),

TextInput_224_cast AS (

  SELECT CAST(idf_pers_ods AS STRING) AS idf_pers_ods
  
  FROM TextInput_224 AS in0

),

Union_159 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_172_left', 'TextInput_224_cast'], 
      [
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "Timestamp"}, {"name": "trato_caja", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "destino_contacto", "dataType": "String"}]', 
        '[{"name": "idf_pers_ods", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_159
