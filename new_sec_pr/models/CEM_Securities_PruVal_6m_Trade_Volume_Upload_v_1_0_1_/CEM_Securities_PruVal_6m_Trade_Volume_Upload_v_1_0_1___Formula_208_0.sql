{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_57 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Configuration_t_57_ref') }}

),

Summarize_231 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1___Summarize_231')}}

),

Join_63_left AS (

  SELECT in0.*
  
  FROM Summarize_231 AS in0
  ANTI JOIN Configuration_t_57 AS in1
     ON ((in0.Instrument = in1.`INSTRUMENT CODE`) AND (in0.ISIN = in1.ISIN))

),

Summarize_207 AS (

  SELECT 
    DISTINCT Instrument AS Instrument,
    ISIN AS ISIN
  
  FROM Join_63_left AS in0

),

Formula_208_0 AS (

  SELECT 
    CAST('Missing Product type' AS string) AS Name,
    *
  
  FROM Summarize_207 AS in0

)

SELECT *

FROM Formula_208_0
