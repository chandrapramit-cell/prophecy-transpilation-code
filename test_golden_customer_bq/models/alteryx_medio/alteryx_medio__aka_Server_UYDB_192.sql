{{
  config({    
    "materialized": "table",
    "alias": "aka_Server_UYDB_192_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_Server_UYDB_214 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_214_ref') }}

),

Formula_185_0 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        (
          REGEXP_REPLACE(
            (
              FORMAT(
                '%.0f', 
                CAST(((EXTRACT(YEAR FROM hora_extraccion_ticket) * 100) + EXTRACT(MONTH FROM hora_extraccion_ticket)) AS FLOAT64))
            ), 
            ',', 
            '__THS__')
        ), 
        '__THS__', 
        '')
    ) AS STRING) AS AAAAMM,
    *
  
  FROM aka_Server_UYDB_214 AS in0

),

Sort_186 AS (

  SELECT * 
  
  FROM Formula_185_0 AS in0
  
  ORDER BY hora_extraccion_ticket ASC

),

Summarize_184 AS (

  SELECT 
    (FIRST_VALUE(codigo_team) OVER (PARTITION BY idf_pers_ods, AAAAMM ORDER BY hora_extraccion_ticket RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS codigo_team_tmp,
    (ROW_NUMBER() OVER (PARTITION BY idf_pers_ods, AAAAMM)) AS row_number,
    *
  
  FROM Sort_186 AS in0

),

Summarize_184_rename AS (

  SELECT 
    codigo_team_tmp AS codigo_team,
    * EXCEPT (`codigo_team`, `codigo_team_tmp`)
  
  FROM Summarize_184 AS in0

),

`184_filter` AS (

  SELECT * 
  
  FROM Summarize_184_rename AS in0
  
  WHERE (row_number = 1)

),

Summarize_184_drop_0 AS (

  SELECT * EXCEPT (`row_number`)
  
  FROM `184_filter` AS in0

),

aka_Server_UYDB_220 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_220_ref') }}

),

Union_170 AS (

  SELECT *
  
  FROM {{ ref('alteryx_medio__Union_170')}}

),

Join_172_inner AS (

  SELECT 
    in0.* EXCEPT (`descripcion_encuesta`),
    in1.* EXCEPT (`descripcion_encuesta_sistema`)
  
  FROM Union_170 AS in0
  INNER JOIN aka_Server_UYDB_220 AS in1
     ON (in0.descripcion_encuesta = in1.descripcion_encuesta_sistema)

),

Union_173_reformat_3 AS (

  SELECT 
    CAST(codigo_encuesta AS STRING) AS codigo_encuesta,
    descripcion_encuesta AS descripcion_encuesta,
    CAST(destino_contacto AS STRING) AS destino_contacto,
    encuestadora AS encuestadora,
    CAST(fecha_hora_respuesta AS STRING) AS fecha_hora_respuesta,
    CAST(grupo_encuesta AS STRING) AS grupo_encuesta,
    idf_pers_ods AS idf_pers_ods,
    motivo_nps AS motivo_nps,
    CAST(motivo_nps_ejecutivo AS STRING) AS motivo_nps_ejecutivo,
    nps AS nps,
    CAST(nps_ejecutivo AS STRING) AS nps_ejecutivo,
    CAST(trato_caja AS STRING) AS trato_caja,
    CAST(trato_ejecutivo AS STRING) AS trato_ejecutivo,
    CAST(trato_mostrador AS STRING) AS trato_mostrador
  
  FROM Join_172_inner AS in0

),

Join_172_left AS (

  SELECT *
  
  FROM {{ ref('alteryx_medio__Join_172_left')}}

),

Union_173_reformat_2 AS (

  SELECT 
    CAST(codigo_encuesta AS STRING) AS codigo_encuesta,
    descripcion_encuesta AS descripcion_encuesta,
    CAST(destino_contacto AS STRING) AS destino_contacto,
    encuestadora AS encuestadora,
    CAST(fecha_hora_respuesta AS STRING) AS fecha_hora_respuesta,
    idf_pers_ods AS idf_pers_ods,
    motivo_nps AS motivo_nps,
    CAST(motivo_nps_ejecutivo AS STRING) AS motivo_nps_ejecutivo,
    nps AS nps,
    CAST(nps_ejecutivo AS STRING) AS nps_ejecutivo,
    CAST(trato_caja AS STRING) AS trato_caja,
    CAST(trato_ejecutivo AS STRING) AS trato_ejecutivo,
    CAST(trato_mostrador AS STRING) AS trato_mostrador
  
  FROM Join_172_left AS in0

),

aka_Server_UYDB_221 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_221_ref') }}

),

AlteryxSelect_155 AS (

  SELECT 
    idf_pers_ods AS idf_pers_ods,
    encuestadora AS encuestadora,
    descripcion_encuesta AS descripcion_encuesta,
    nps AS nps,
    grupo_encuesta AS grupo_encuesta,
    motivo_nps AS motivo_nps,
    fecha_hora_respuesta AS fecha_hora_respuesta
  
  FROM aka_Server_UYDB_221 AS in0

),

Union_173_reformat_0 AS (

  SELECT 
    descripcion_encuesta AS descripcion_encuesta,
    encuestadora AS encuestadora,
    CAST(fecha_hora_respuesta AS STRING) AS fecha_hora_respuesta,
    CAST(grupo_encuesta AS STRING) AS grupo_encuesta,
    idf_pers_ods AS idf_pers_ods,
    motivo_nps AS motivo_nps,
    nps AS nps
  
  FROM AlteryxSelect_155 AS in0

),

aka_Server_UYDB_223 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_223_ref') }}

),

AlteryxSelect_162 AS (

  SELECT 
    CAST(idf_pers_ods AS STRING) AS idf_pers_ods,
    encuestadora AS encuestadora,
    codigoEncuesta AS codigo_encuesta,
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(fecha_respuesta AS STRING)) IS NOT NULL)
          THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(fecha_respuesta AS STRING))
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(fecha_respuesta AS STRING)) IS NOT NULL)
          THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(fecha_respuesta AS STRING))
        ELSE SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(fecha_respuesta AS STRING))
      END
    ) AS fecha_hora_respuesta,
    destinoContacto AS destino_contacto,
    nps AS nps,
    descripcionEncuesta AS descripcion_encuesta,
    motivoNps AS motivo_nps,
    CAST(npsEjecutivo AS STRING) AS nps_ejecutivo,
    motivoNpsEjecutivo AS motivo_nps_ejecutivo,
    CAST(tratoEjecutivo AS STRING) AS trato_ejecutivo,
    CAST(tratoMostrador AS STRING) AS trato_mostrador
  
  FROM aka_Server_UYDB_223 AS in0

),

Union_173_reformat_1 AS (

  SELECT 
    CAST(codigo_encuesta AS STRING) AS codigo_encuesta,
    descripcion_encuesta AS descripcion_encuesta,
    CAST(destino_contacto AS STRING) AS destino_contacto,
    encuestadora AS encuestadora,
    CAST(fecha_hora_respuesta AS STRING) AS fecha_hora_respuesta,
    idf_pers_ods AS idf_pers_ods,
    motivo_nps AS motivo_nps,
    CAST(motivo_nps_ejecutivo AS STRING) AS motivo_nps_ejecutivo,
    nps AS nps,
    CAST(nps_ejecutivo AS STRING) AS nps_ejecutivo,
    CAST(trato_ejecutivo AS STRING) AS trato_ejecutivo,
    CAST(trato_mostrador AS STRING) AS trato_mostrador
  
  FROM AlteryxSelect_162 AS in0

),

Union_173 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_173_reformat_0', 'Union_173_reformat_1', 'Union_173_reformat_3', 'Union_173_reformat_2'], 
      [
        '[{"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "Timestamp"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}]', 
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "destino_contacto", "dataType": "String"}]', 
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "Timestamp"}, {"name": "trato_caja", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "destino_contacto", "dataType": "String"}]', 
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "Timestamp"}, {"name": "trato_caja", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "destino_contacto", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_171 AS (

  select *  REPLACE( idf_pers_ods as `idf_pers_ods` ,  encuestadora as `encuestadora` ,  codigo_encuesta as `codigo_encuesta` ,  fecha_hora_respuesta as `fecha_hora_respuesta` ,  destino_contacto as `destino_contacto` ,  nps as `nps` ,  descripcion_encuesta as `descripcion_encuesta` ,  grupo_encuesta as `grupo_encuesta` ,  motivo_nps as `motivo_nps` ,  nps_ejecutivo as `nps_ejecutivo` ,  motivo_nps_ejecutivo as `motivo_nps_ejecutivo` ,  trato_ejecutivo as `trato_ejecutivo` ,  trato_mostrador as `trato_mostrador` ,  trato_caja as `trato_caja` ) from Union_173

),

Filter_198 AS (

  SELECT * 
  
  FROM AlteryxSelect_171 AS in0
  
  WHERE (
          (
            (
              (NOT CAST(((STRPOS((coalesce(LOWER(nps), '')), LOWER(';'))) > 0) AS BOOL))
              AND ((motivo_nps <> 'Lia') OR (motivo_nps IS NULL))
            )
            AND ((motivo_nps <> 'Lia') OR (motivo_nps IS NULL))
          )
          AND ((motivo_nps <> 'Diego') OR (motivo_nps IS NULL))
        )

),

Formula_195_0 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        (
          REGEXP_REPLACE(
            (
              FORMAT(
                '%.0f', 
                CAST((
                  (EXTRACT(YEAR FROM (PARSE_DATE('%Y-%m-%d', fecha_hora_respuesta))) * 100)
                  + EXTRACT(MONTH FROM (PARSE_DATE('%Y-%m-%d', fecha_hora_respuesta)))
                ) AS FLOAT64))
            ), 
            ',', 
            '__THS__')
        ), 
        '__THS__', 
        '')
    ) AS STRING) AS AAAAMM,
    CAST((
      REGEXP_REPLACE(
        (
          REGEXP_REPLACE(
            (
              FORMAT(
                '%.0f', 
                CAST((
                  (EXTRACT(YEAR FROM (DATE_ADD(fecha_hora_respuesta, INTERVAL -1 MONTH))) * 100)
                  + EXTRACT(MONTH FROM (DATE_ADD(fecha_hora_respuesta, INTERVAL -1 MONTH)))
                ) AS FLOAT64))
            ), 
            ',', 
            '__THS__')
        ), 
        '__THS__', 
        '')
    ) AS STRING) AS `AAAAMM-1`,
    *
  
  FROM Filter_198 AS in0

),

Filter_199_reject AS (

  SELECT * 
  
  FROM Formula_195_0 AS in0
  
  WHERE (
          (
            ((descripcion_encuesta <> 'Monitorizacion Sucursales') OR (descripcion_encuesta IS NULL))
            OR ((descripcion_encuesta = 'Monitorizacion Sucursales') IS NULL)
          )
          AND (
                NOT(
                  (descripcion_encuesta = 'Monitorizacion Corporate')
                  OR (descripcion_encuesta = 'Monitorizacion Select'))
              )
        )

),

Join_205_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`idf_pers_ods`, `AAAAMM`)
  
  FROM Filter_199_reject AS in0
  INNER JOIN Summarize_184_drop_0 AS in1
     ON ((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.AAAAMM = in1.AAAAMM))

),

aka_Server_UYDB_215 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_215_ref') }}

),

Formula_180_0 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        (
          REGEXP_REPLACE(
            (FORMAT('%.0f', CAST(((EXTRACT(YEAR FROM fecha_dato) * 100) + EXTRACT(MONTH FROM fecha_dato)) AS FLOAT64))), 
            ',', 
            '__THS__')
        ), 
        '__THS__', 
        '')
    ) AS STRING) AS AAAAMM,
    *
  
  FROM aka_Server_UYDB_215 AS in0

),

Sort_181 AS (

  SELECT * 
  
  FROM Formula_180_0 AS in0
  
  ORDER BY fecha_dato ASC

),

Summarize_182 AS (

  SELECT 
    (FIRST_VALUE(codigo_team_actual) OVER (PARTITION BY codigo_oficial, AAAAMM ORDER BY fecha_dato RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS CodTeamBusqueda,
    (ROW_NUMBER() OVER (PARTITION BY codigo_oficial, AAAAMM)) AS row_number,
    *
  
  FROM Sort_181 AS in0

),

aka_Server_UYDB_216 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_216_ref') }}

),

aka_Server_UYDB_213 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_213_ref') }}

),

Summarize_175 AS (

  SELECT MAX(AAAAMM) AS AAAAMM
  
  FROM aka_Server_UYDB_213 AS in0

),

Formula_176_0 AS (

  select *  REPLACE( CAST((CASE WHEN (CAST((SUBSTRING( AAAAMM, (((LENGTH(AAAAMM)) - 2) + 1), 2)) AS STRING) = '12') THEN (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', ((coalesce(CAST(AAAAMM AS FLOAT64), CAST((REGEXP_EXTRACT(AAAAMM, '^[0-9]+', 0)) AS INT64), 0)) + 100))), ',', '__THS__')), '__THS__', '')) ELSE (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', ((coalesce(CAST(AAAAMM AS FLOAT64), CAST((REGEXP_EXTRACT(AAAAMM, '^[0-9]+', 0)) AS INT64), 0)) + 1))), ',', '__THS__')), '__THS__', '')) END) AS STRING) as `AAAAMM` ) from Summarize_175

),

AppendFields_177 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Formula_176_0 AS in0
  INNER JOIN aka_Server_UYDB_216 AS in1
     ON true

),

Union_178 AS (

  {{
    prophecy_basics.UnionByName(
      ['AppendFields_177', 'aka_Server_UYDB_213'], 
      [
        '[{"name": "AAAAMM", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "codigo_oficial_comercial", "dataType": "String"}, {"name": "codigo_subsegmento", "dataType": "String"}, {"name": "codigo_segmento", "dataType": "String"}]', 
        '[{"name": "codigo_segmento", "dataType": "String"}, {"name": "codigo_oficial_comercial", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "codigo_subsegmento", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_194 AS (

  SELECT * 
  
  FROM Formula_195_0 AS in0
  
  WHERE (
          (descripcion_encuesta = 'Monitorizacion Corporate')
          OR (descripcion_encuesta = 'Monitorizacion Select')
        )

),

Join_196_left AS (

  SELECT in0.*
  
  FROM Filter_194 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.AAAAMM,
      in1.idf_pers_ods
    
    FROM Union_178 AS in1
    
    WHERE in1.AAAAMM IS NOT NULL AND in1.idf_pers_ods IS NOT NULL
  ) AS in1_keys
     ON ((in0.AAAAMM = in1_keys.AAAAMM) AND (in0.idf_pers_ods = in1_keys.idf_pers_ods))
  
  WHERE (in1_keys.AAAAMM IS NULL)

),

Join_197_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`idf_pers_ods`, `AAAAMM`)
  
  FROM Join_196_left AS in0
  LEFT JOIN Union_178 AS in1
     ON ((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.`AAAAMM-1` = in1.AAAAMM))

),

`182_filter` AS (

  SELECT * 
  
  FROM Summarize_182 AS in0
  
  WHERE (row_number = 1)

),

Summarize_182_drop_0 AS (

  SELECT * EXCEPT (`row_number`)
  
  FROM `182_filter` AS in0

),

Join_196_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`idf_pers_ods`, `AAAAMM`)
  
  FROM Filter_194 AS in0
  INNER JOIN Union_178 AS in1
     ON ((in0.AAAAMM = in1.AAAAMM) AND (in0.idf_pers_ods = in1.idf_pers_ods))

),

Union_202 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_196_inner', 'Join_197_left_UnionLeftOuter'], 
      [
        '[{"name": "codigo_segmento", "dataType": "String"}, {"name": "codigo_oficial_comercial", "dataType": "String"}, {"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "trato_caja", "dataType": "String"}, {"name": "AAAAMM-1", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "codigo_subsegmento", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "destino_contacto", "dataType": "String"}]', 
        '[{"name": "codigo_segmento", "dataType": "String"}, {"name": "codigo_oficial_comercial", "dataType": "String"}, {"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "trato_caja", "dataType": "String"}, {"name": "AAAAMM-1", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "codigo_subsegmento", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "destino_contacto", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_199 AS (

  SELECT * 
  
  FROM Formula_195_0 AS in0
  
  WHERE (
          (descripcion_encuesta = 'Monitorizacion Sucursales')
          AND (
                NOT(
                  (descripcion_encuesta = 'Monitorizacion Corporate')
                  OR (descripcion_encuesta = 'Monitorizacion Select'))
              )
        )

),

Join_200_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`idf_pers_ods`, `AAAAMM`)
  
  FROM Filter_199 AS in0
  INNER JOIN Summarize_184_drop_0 AS in1
     ON ((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.AAAAMM = in1.AAAAMM))

),

Join_200_left AS (

  SELECT in0.*
  
  FROM Filter_199 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.idf_pers_ods,
      in1.AAAAMM
    
    FROM Summarize_184_drop_0 AS in1
    
    WHERE in1.idf_pers_ods IS NOT NULL AND in1.AAAAMM IS NOT NULL
  ) AS in1_keys
     ON ((in0.idf_pers_ods = in1_keys.idf_pers_ods) AND (in0.AAAAMM = in1_keys.AAAAMM))
  
  WHERE (in1_keys.idf_pers_ods IS NULL)

),

Join_201_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`idf_pers_ods`, `AAAAMM`)
  
  FROM Join_200_left AS in0
  LEFT JOIN Summarize_184_drop_0 AS in1
     ON ((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.`AAAAMM-1` = in1.AAAAMM))

),

Union_203 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_200_inner', 'Join_201_left_UnionLeftOuter'], 
      [
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "trato_caja", "dataType": "String"}, {"name": "codigo_team", "dataType": "String"}, {"name": "AAAAMM-1", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "hora_extraccion_ticket", "dataType": "Timestamp"}, {"name": "destino_contacto", "dataType": "String"}]', 
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "trato_caja", "dataType": "String"}, {"name": "codigo_team", "dataType": "String"}, {"name": "AAAAMM-1", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "hora_extraccion_ticket", "dataType": "Timestamp"}, {"name": "destino_contacto", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_205_left AS (

  SELECT in0.*
  
  FROM Filter_199_reject AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.idf_pers_ods,
      in1.AAAAMM
    
    FROM Summarize_184_drop_0 AS in1
    
    WHERE in1.idf_pers_ods IS NOT NULL AND in1.AAAAMM IS NOT NULL
  ) AS in1_keys
     ON ((in0.idf_pers_ods = in1_keys.idf_pers_ods) AND (in0.AAAAMM = in1_keys.AAAAMM))
  
  WHERE (in1_keys.idf_pers_ods IS NULL)

),

Join_206_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`idf_pers_ods`, `AAAAMM`)
  
  FROM Join_205_left AS in0
  LEFT JOIN Summarize_184_drop_0 AS in1
     ON ((in0.idf_pers_ods = in1.idf_pers_ods) AND (in0.`AAAAMM-1` = in1.AAAAMM))

),

Union_207 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_205_inner', 'Join_206_left_UnionLeftOuter'], 
      [
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "trato_caja", "dataType": "String"}, {"name": "codigo_team", "dataType": "String"}, {"name": "AAAAMM-1", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "hora_extraccion_ticket", "dataType": "Timestamp"}, {"name": "destino_contacto", "dataType": "String"}]', 
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "trato_caja", "dataType": "String"}, {"name": "codigo_team", "dataType": "String"}, {"name": "AAAAMM-1", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "hora_extraccion_ticket", "dataType": "Timestamp"}, {"name": "destino_contacto", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_204 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_202', 'Union_203', 'Union_207'], 
      [
        '[{"name": "codigo_segmento", "dataType": "String"}, {"name": "codigo_oficial_comercial", "dataType": "String"}, {"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "trato_caja", "dataType": "String"}, {"name": "AAAAMM-1", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "codigo_subsegmento", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "destino_contacto", "dataType": "String"}]', 
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "trato_caja", "dataType": "String"}, {"name": "codigo_team", "dataType": "String"}, {"name": "AAAAMM-1", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "hora_extraccion_ticket", "dataType": "Timestamp"}, {"name": "destino_contacto", "dataType": "String"}]', 
        '[{"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "encuestadora", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "trato_caja", "dataType": "String"}, {"name": "codigo_team", "dataType": "String"}, {"name": "AAAAMM-1", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "hora_extraccion_ticket", "dataType": "Timestamp"}, {"name": "destino_contacto", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_208_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`codigo_oficial`, `AAAAMM`)
  
  FROM Union_204 AS in0
  INNER JOIN Summarize_182_drop_0 AS in1
     ON ((in0.codigo_oficial_comercial = in1.codigo_oficial) AND (in0.AAAAMM = in1.AAAAMM))

),

Join_208_left AS (

  SELECT in0.*
  
  FROM Union_204 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.codigo_oficial,
      in1.AAAAMM
    
    FROM Summarize_182_drop_0 AS in1
    
    WHERE in1.codigo_oficial IS NOT NULL AND in1.AAAAMM IS NOT NULL
  ) AS in1_keys
     ON ((in0.codigo_oficial_comercial = in1_keys.codigo_oficial) AND (in0.AAAAMM = in1_keys.AAAAMM))
  
  WHERE (in1_keys.codigo_oficial IS NULL)

),

Join_209_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`codigo_oficial`, `AAAAMM`)
  
  FROM Join_208_left AS in0
  LEFT JOIN Summarize_182_drop_0 AS in1
     ON ((in0.codigo_oficial_comercial = in1.codigo_oficial) AND (in0.`AAAAMM-1` = in1.AAAAMM))

),

Union_210 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_208_inner', 'Join_209_left_UnionLeftOuter'], 
      [
        '[{"name": "codigo_segmento", "dataType": "String"}, {"name": "codigo_oficial_comercial", "dataType": "String"}, {"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "fecha_dato", "dataType": "Date"}, {"name": "encuestadora", "dataType": "String"}, {"name": "CodTeamBusqueda", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "trato_caja", "dataType": "String"}, {"name": "codigo_team", "dataType": "String"}, {"name": "AAAAMM-1", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "codigo_team_actual", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "codigo_subsegmento", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "hora_extraccion_ticket", "dataType": "Timestamp"}, {"name": "destino_contacto", "dataType": "String"}]', 
        '[{"name": "codigo_segmento", "dataType": "String"}, {"name": "codigo_oficial_comercial", "dataType": "String"}, {"name": "nps_ejecutivo", "dataType": "String"}, {"name": "descripcion_encuesta", "dataType": "String"}, {"name": "fecha_dato", "dataType": "Date"}, {"name": "encuestadora", "dataType": "String"}, {"name": "CodTeamBusqueda", "dataType": "String"}, {"name": "trato_ejecutivo", "dataType": "String"}, {"name": "grupo_encuesta", "dataType": "String"}, {"name": "codigo_encuesta", "dataType": "String"}, {"name": "fecha_hora_respuesta", "dataType": "String"}, {"name": "trato_caja", "dataType": "String"}, {"name": "codigo_team", "dataType": "String"}, {"name": "AAAAMM-1", "dataType": "String"}, {"name": "motivo_nps_ejecutivo", "dataType": "String"}, {"name": "codigo_team_actual", "dataType": "String"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "nps", "dataType": "String"}, {"name": "trato_mostrador", "dataType": "String"}, {"name": "codigo_subsegmento", "dataType": "String"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "motivo_nps", "dataType": "String"}, {"name": "hora_extraccion_ticket", "dataType": "Timestamp"}, {"name": "destino_contacto", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_211_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (codigo_team IS NULL)
          THEN CodTeamBusqueda
        ELSE codigo_team
      END
    ) AS STRING) AS codigo_team,
    * EXCEPT (`codigo_team`)
  
  FROM Union_210 AS in0

),

AlteryxSelect_156 AS (

  SELECT 
    AAAAMM AS AAAAMM,
    idf_pers_ods AS idf_pers_ods,
    encuestadora AS encuestadora,
    codigo_encuesta AS codigo_encuesta,
    descripcion_encuesta AS descripcion_encuesta,
    grupo_encuesta AS grupo_encuesta,
    fecha_hora_respuesta AS fecha_hora_respuesta,
    destino_contacto AS destino_contacto,
    nps AS nps,
    motivo_nps AS motivo_nps,
    nps_ejecutivo AS nps_ejecutivo,
    motivo_nps_ejecutivo AS motivo_nps_ejecutivo,
    trato_ejecutivo AS trato_ejecutivo,
    trato_mostrador AS trato_mostrador,
    trato_caja AS trato_caja,
    codigo_team AS codigo_team,
    codigo_oficial_comercial AS codigo_oficial_comercial,
    codigo_segmento AS codigo_segmento,
    codigo_subsegmento AS codigo_subsegmento,
    * EXCEPT (`AAAAMM-1`, 
    `CodTeamBusqueda`, 
    `AAAAMM`, 
    `idf_pers_ods`, 
    `encuestadora`, 
    `codigo_encuesta`, 
    `descripcion_encuesta`, 
    `grupo_encuesta`, 
    `fecha_hora_respuesta`, 
    `destino_contacto`, 
    `nps`, 
    `motivo_nps`, 
    `nps_ejecutivo`, 
    `motivo_nps_ejecutivo`, 
    `trato_ejecutivo`, 
    `trato_mostrador`, 
    `trato_caja`, 
    `codigo_team`, 
    `codigo_oficial_comercial`, 
    `codigo_segmento`, 
    `codigo_subsegmento`)
  
  FROM Formula_211_0 AS in0

),

MultiFieldFormula_161 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['AlteryxSelect_156'], 
      "CASE WHEN (column_value = '') THEN NULL ELSE column_value END", 
      [
        'codigo_segmento', 
        'codigo_oficial_comercial', 
        'nps_ejecutivo', 
        'descripcion_encuesta', 
        'fecha_dato', 
        'encuestadora', 
        'trato_ejecutivo', 
        'grupo_encuesta', 
        'codigo_encuesta', 
        'fecha_hora_respuesta', 
        'trato_caja', 
        'codigo_team', 
        'motivo_nps_ejecutivo', 
        'codigo_team_actual', 
        'idf_pers_ods', 
        'nps', 
        'trato_mostrador', 
        'codigo_subsegmento', 
        'AAAAMM', 
        'motivo_nps', 
        'hora_extraccion_ticket', 
        'destino_contacto'
      ], 
      ['nps', 'motivo_nps', 'nps_ejecutivo', 'motivo_nps_ejecutivo', 'trato_ejecutivo', 'trato_mostrador'], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Filter_157 AS (

  SELECT * 
  
  FROM MultiFieldFormula_161 AS in0
  
  WHERE (
          (
            (
              (
                (NOT((nps IS NULL) OR ((LENGTH(nps)) = 0)))
                OR (NOT((nps_ejecutivo IS NULL) OR ((LENGTH(nps_ejecutivo)) = 0)))
              )
              OR (NOT((trato_ejecutivo IS NULL) OR ((LENGTH(trato_ejecutivo)) = 0)))
            )
            OR (NOT((trato_mostrador IS NULL) OR ((LENGTH(trato_mostrador)) = 0)))
          )
          OR (NOT((trato_caja IS NULL) OR ((LENGTH(trato_caja)) = 0)))
        )

),

AlteryxSelect_163 AS (

  SELECT 
    AAAAMM AS AAAAMM,
    idf_pers_ods AS idf_pers_ods,
    encuestadora AS encuestadora,
    codigo_encuesta AS codigo_encuesta,
    descripcion_encuesta AS descripcion_encuesta,
    grupo_encuesta AS grupo_encuesta,
    fecha_hora_respuesta AS fecha_hora_respuesta,
    destino_contacto AS destino_contacto,
    nps AS nps,
    motivo_nps AS motivo_nps,
    nps_ejecutivo AS nps_ejecutivo,
    motivo_nps_ejecutivo AS motivo_nps_ejecutivo,
    trato_ejecutivo AS trato_ejecutivo,
    trato_mostrador AS trato_mostrador,
    trato_caja AS trato_caja,
    codigo_team AS codigo_team,
    codigo_oficial_comercial AS codigo_oficial_comercial,
    codigo_segmento AS codigo_segmento,
    codigo_subsegmento AS codigo_subsegmento
  
  FROM Filter_157 AS in0

)

SELECT *

FROM AlteryxSelect_163
