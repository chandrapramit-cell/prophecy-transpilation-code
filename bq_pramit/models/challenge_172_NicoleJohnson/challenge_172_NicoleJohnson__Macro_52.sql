{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TopBlocks_yxdb_23 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('challenge_172_NicoleJohnson', 'TopBlocks_yxdb_23') }}

),

Summarize_54 AS (

  {#Combines multiple spatial blocks into a single geographic object for aggregated location analysis.#}
  SELECT (ST_ASTEXT((ST_UNION_AGG((ST_GEOGFROMTEXT(SpatialObj)))))) AS SpatialObjCombine_SpatialObj
  
  FROM TopBlocks_yxdb_23 AS in0

),

PolySplit_55 AS (

  {{
    prophecy_basics.ToDo(
      'Component type: AlteryxSpatialPluginsGui.PolySplit.PolySplit is not supported.'
    )
  }}

),

RecordID_58 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM PolySplit_55

),

SpatialMatch_56 AS (

  {{ prophecy_basics.ToDo('SpatialMatch is not supported.') }}

),

Summarize_57 AS (

  SELECT 
    SUM(`Total HH`) AS `Sum_Total HH`,
    RecordID AS RecordID
  
  FROM SpatialMatch_56 AS in0
  
  GROUP BY RecordID

),

Join_59_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`RecordID`)
  
  FROM Summarize_57 AS in0
  INNER JOIN RecordID_58 AS in1
     ON (in0.RecordID = in1.RecordID)

),

Formula_60_0 AS (

  SELECT 
    CEIL((`Sum_Total HH` / 160)) AS NumberVolunteers,
    *
  
  FROM Join_59_inner AS in0

),

Macro_52 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Final_Heat3 Answer Check_Encrypted.yxmc to resolve it.'
    )
  }}

)

SELECT *

FROM Macro_52
