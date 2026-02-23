{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH aka_Server_UYDB_222 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_Server_UYDB_222_ref') }}

),

Filter_234 AS (

  SELECT * 
  
  FROM aka_Server_UYDB_222 AS in0
  
  WHERE (codigo_proceso = 'C001004')

),

Formula_237_0 AS (

  SELECT 
    'lista_distribucion' AS lista,
    *
  
  FROM Filter_234 AS in0

),

Union_159 AS (

  SELECT *
  
  FROM {{ ref('alteryx_medio__Union_159')}}

),

Summarize_228 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((idf_pers_ods IS NULL) OR (CAST(idf_pers_ods AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    descripcion_encuesta AS descripcion_encuesta
  
  FROM Union_159 AS in0
  
  GROUP BY descripcion_encuesta

),

Filter_232 AS (

  SELECT * 
  
  FROM Summarize_228 AS in0
  
  WHERE ((LENGTH(descripcion_encuesta)) <> 0)

),

PortfolioComposerTable_229 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

PortfolioComposerText_230 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

PortfolioComposerLayout_231 AS (

  {{ prophecy_basics.ToDo('Component type: Layout is not supported.') }}

),

CrossTab_235 AS (

  SELECT *
  
  FROM (
    SELECT 
      grupo_distribucion,
      lista,
      MAIL
    
    FROM Formula_237_0 AS in0
  )
  PIVOT (
    CONCAT_WS(MAIL) AS Concat
    FOR lista
    IN (
      'lista_distribucion'
    )
  )

),

AppendFields_227 AS (

  SELECT 
    in0.* EXCEPT (`grupo_distribucion`),
    in1.*
  
  FROM CrossTab_235 AS in0
  INNER JOIN PortfolioComposerLayout_231 AS in1
     ON true

)

SELECT *

FROM AppendFields_227
