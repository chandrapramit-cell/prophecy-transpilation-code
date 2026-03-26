{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_25 AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___AlteryxSelect_25')}}

),

Emailrecipients_59 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1_', 'Emailrecipients_59') }}

),

Filter_63 AS (

  SELECT * 
  
  FROM Emailrecipients_59 AS in0
  
  WHERE (TOSLASHCC = 'Cc')

),

Summarize_66 AS (

  SELECT listagg("EMAIL ADDRESS", ';') AS "EMAIL ADDRESS"
  
  FROM Filter_63 AS in0

),

Unique_51 AS (

  SELECT * 
  
  FROM AlteryxSelect_25 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY FISCAL_YEAR, PERIOD, LEDGER, COMPANY_CODE ORDER BY FISCAL_YEAR, PERIOD, LEDGER, COMPANY_CODE) = 1

),

Filter_62 AS (

  SELECT * 
  
  FROM Emailrecipients_59 AS in0
  
  WHERE (TOSLASHCC = 'To')

),

PortfolioComposerTable_55 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

Summarize_65 AS (

  SELECT listagg("EMAIL ADDRESS", '; ') AS "EMAIL ADDRESS"
  
  FROM Filter_62 AS in0

),

PortfolioComposerText_57 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

AppendFields_60 AS (

  SELECT 
    in0."EMAIL ADDRESS" AS TO_EMAIL_ADDRESS,
    in0.* EXCLUDE ("EMAIL ADDRESS"),
    in1.*
  
  FROM Summarize_65 AS in0
  INNER JOIN PortfolioComposerText_57 AS in1
     ON TRUE

),

AppendFields_64 AS (

  SELECT 
    in0."EMAIL ADDRESS" AS CC_EMAIL_ADDRESS,
    in0.* EXCLUDE ("EMAIL ADDRESS"),
    in1.*
  
  FROM Summarize_66 AS in0
  INNER JOIN AppendFields_60 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_64
