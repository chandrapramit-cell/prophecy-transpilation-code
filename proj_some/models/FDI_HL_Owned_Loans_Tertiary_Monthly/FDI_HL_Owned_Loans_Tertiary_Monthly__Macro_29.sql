{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AlteryxSelect_33 AS (

  SELECT *
  
  FROM {{ ref('FDI_HL_Owned_Loans_Tertiary_Monthly__AlteryxSelect_33')}}

),

Macro_29 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file /Naeast.ad.jpmorganchase.com/corp2/FIRIDC/NACORP2FIRIDCSHARE00001/FDIALTERYX/RELEASE/TABLEAU/JPMC_Publish_to_Tableau_Hyper_v3_FDI/JPMC_Publish_to_Tableau_Hyper_v3_CT.yxmc to resolve it.'
    )
  }}

)

SELECT *

FROM Macro_29
