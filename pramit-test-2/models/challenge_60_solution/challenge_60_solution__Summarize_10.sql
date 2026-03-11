{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_1 AS (

  SELECT * 
  
  FROM {{ ref('seed_1')}}

),

TextInput_1_cast AS (

  SELECT 
    CAST(Label AS STRING) AS Label,
    CAST(SpatialObj AS STRING) AS SpatialObj
  
  FROM TextInput_1 AS in0

),

PolySplit_8 AS (

  {{
    prophecy_basics.ToDo(
      'Component type: AlteryxSpatialPluginsGui.PolySplit.PolySplit is not supported.'
    )
  }}

),

Summarize_10 AS (

  SELECT 
    (ST_ASTEXT((ST_UNION_AGG((ST_GEOMFROMTEXT(Split_SpatialObj)))))) AS `Spatial Object Final`,
    Label AS Label
  
  FROM PolySplit_8 AS in0
  
  GROUP BY Label

)

SELECT *

FROM Summarize_10
