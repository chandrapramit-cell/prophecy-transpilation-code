{{
  config({    
    "materialized": "table",
    "alias": "table_3118_Loop_macro_op",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AppendFields_842_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__AppendFields_842_3118')}}

),

Filter_851_3118 AS (

  SELECT * 
  
  FROM AppendFields_842_3118 AS in0
  
  WHERE (CAST('{{ var('iteration_number') }}' AS DOUBLE) < (MaxIteration - 1))

),

AlteryxSelect_850_3118 AS (

  SELECT * EXCEPT (`StaticHistoryMonth`, `MaxIteration`, `StaticHistoryYearEnd`)
  
  FROM Filter_851_3118 AS in0

)

SELECT *

FROM AlteryxSelect_850_3118
