{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH aka_Server_UYDB_219 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_219_ref') }}

),

Filter_164 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_219 AS in0
  
  WHERE (NOT((tipo_respuesta IS NULL) OR ((LENGTH(tipo_respuesta)) = 0)))

),

aka_Server_UYDB_218 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_218_ref') }}

),

Unique_168 AS (

  SELECT * 
  
  FROM Filter_164 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY codigo_encuesta, codigo_pregunta ORDER BY codigo_encuesta, codigo_pregunta) = 1

),

Join_165_inner AS (

  SELECT 
    in0.descripcion_encuesta AS descripcion_encuesta,
    in0.encuestadora AS encuestadora,
    in0.codigo_encuesta AS codigo_encuesta,
    in1.tipo_respuesta AS tipo_respuesta,
    in0.respuesta AS respuesta,
    in0.fecha_hora_respuesta AS fecha_hora_respuesta,
    in0.codigo_pregunta AS codigo_pregunta,
    in0.idf_pers_ods AS idf_pers_ods,
    in0.destino_contacto AS destino_contacto
  
  FROM aka_Server_UYDB_218 AS in0
  INNER JOIN Unique_168 AS in1
     ON ((in0.codigo_encuesta = in1.codigo_encuesta) AND (in0.codigo_pregunta = in1.codigo_pregunta))

),

CrossTab_167 AS (

  SELECT *
  
  FROM (
    SELECT 
      idf_pers_ods,
      encuestadora,
      codigo_encuesta,
      descripcion_encuesta,
      fecha_hora_respuesta,
      destino_contacto,
      tipo_respuesta,
      RESPUESTA
    
    FROM Join_165_inner AS in0
  )
  PIVOT (
    CONCAT_WS(RESPUESTA) AS Concat
    FOR tipo_respuesta
    IN (
      'MOTIVO', 'MOTIVO_EJECUTIVO', 'TRATO_CAJA', 'TRATO_MOSTRADOR', 'NPS_EJECUTIVO', 'NPS', 'TRATO_EJECUTIVO'
    )
  )

),

AlteryxSelect_166 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    encuestadora AS encuestadora,
    codigo_encuesta AS codigo_encuesta,
    descripcion_encuesta AS descripcion_encuesta,
    fecha_hora_respuesta AS fecha_hora_respuesta,
    destino_contacto AS destino_contacto,
    CAST(NPS AS STRING) AS nps,
    MOTIVO AS motivo_nps,
    CAST(NPS_EJECUTIVO AS STRING) AS nps_ejecutivo,
    MOTIVO_EJECUTIVO AS motivo_nps_ejecutivo,
    TRATO_CAJA AS trato_caja,
    TRATO_EJECUTIVO AS trato_ejecutivo,
    TRATO_MOSTRADOR AS trato_mostrador
  
  FROM CrossTab_167 AS in0

),

aka_Server_UYDB_217 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_217_ref') }}

),

AlteryxSelect_154 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    motivo_nps AS motivo_nps,
    motivo_nps_ejecutivo AS motivo_nps_ejecutivo,
    * EXCEPT (`clave_respuesta`, `idf_pers_ods`, `motivo_nps`, `motivo_nps_ejecutivo`)
  
  FROM aka_Server_UYDB_217 AS in0

),

Union_170 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_154', 'AlteryxSelect_166'], 
      [
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "Timestamp"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "destino_contacto", "dataType": "String"}]', 
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "Timestamp"}, {"name": "trato_caja", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "destino_contacto", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_170
