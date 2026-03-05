{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_24 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__Filter_24')}}

),

Query__Sheet1___144 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DQ_Macro', 'Query__Sheet1___144') }}

),

AlteryxSelect_171 AS (

  SELECT * 
  
  FROM Query__Sheet1___144 AS in0

),

Macro_70 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Field_Summary_Report.yxmc to resolve it.'
    )
  }}

),

Join_72_inner AS (

  SELECT * 
  
  FROM Macro_70 AS in0
  INNER JOIN Filter_24 AS in1
     ON (in0.Name = in1.Name)

)

SELECT *

FROM Join_72_inner
