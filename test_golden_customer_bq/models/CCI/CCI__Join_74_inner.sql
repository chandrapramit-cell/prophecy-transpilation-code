{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_73_0 AS (

  SELECT *
  
  FROM {{ ref('CCI__Formula_73_0')}}

),

Summarize_71 AS (

  SELECT *
  
  FROM {{ ref('CCI__Summarize_71')}}

),

Join_74_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MemberId`, `POINTS`)
  
  FROM Formula_73_0 AS in0
  INNER JOIN Summarize_71 AS in1
     ON (in0.MemberId = in1.MemberId)

)

SELECT *

FROM Join_74_inner
