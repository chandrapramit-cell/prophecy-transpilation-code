{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_482 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Union_482')}}

),

DynamicInput_464 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'DynamicInput_464') }}

),

Join_477_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Reinsurer`, 
    `Account Type`, 
    `Account Name`, 
    `Account Branch`, 
    `CRR`, 
    `Deemed`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`)
  
  FROM Union_482 AS in0
  INNER JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

)

SELECT *

FROM Join_477_inner
