{{
  config({    
    "materialized": "table",
    "alias": "CCI_Scores_yxdb_81_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH Summarize_62 AS (

  SELECT *
  
  FROM {{ ref('CCI__Summarize_62')}}

),

Summarize_71 AS (

  SELECT *
  
  FROM {{ ref('CCI__Summarize_71')}}

),

Formula_73_0 AS (

  SELECT *
  
  FROM {{ ref('CCI__Formula_73_0')}}

),

Join_74_left AS (

  SELECT in0.*
  
  FROM Formula_73_0 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.MemberId
    
    FROM Summarize_71 AS in1
    
    WHERE in1.MemberId IS NOT NULL
  ) AS in1_keys
     ON (in0.MemberId = in1_keys.MemberId)
  
  WHERE (in1_keys.MemberId IS NULL)

),

AppendFields_75 AS (

  SELECT 
    in1.MemberId AS MemberId,
    in0.YR_MO AS YR_MO,
    in1.POINTS AS POINTS
  
  FROM Summarize_62 AS in0
  INNER JOIN Join_74_left AS in1
     ON true

),

Union_76 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_71', 'AppendFields_75'], 
      [
        '[{"name": "MemberId", "dataType": "Integer"}, {"name": "YR_MO", "dataType": "Timestamp"}, {"name": "POINTS", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Integer"}, {"name": "YR_MO", "dataType": "Timestamp"}, {"name": "POINTS", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Sort_79 AS (

  SELECT * 
  
  FROM Union_76 AS in0
  
  ORDER BY MemberId ASC, YR_MO ASC

)

SELECT *

FROM Sort_79
