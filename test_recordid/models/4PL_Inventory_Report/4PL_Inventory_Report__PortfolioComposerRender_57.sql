{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH AlteryxSelect_139 AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report__AlteryxSelect_139')}}

),

CrossTab_67_rename AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('4PL_Inventory_Report', 'CrossTab_67_rename') }}

),

PortfolioComposerTable_55 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

ReportHeader_56 AS (

  {{ prophecy_basics.ToDo('Component type: ReportHeader is not supported.') }}

),

PortfolioComposerText_95 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

PortfolioComposerLayout_59 AS (

  {{ prophecy_basics.ToDo('Component type: Layout is not supported.') }}

),

AppendFields_140 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM AlteryxSelect_139 AS in0
  INNER JOIN PortfolioComposerLayout_59 AS in1
     ON TRUE

),

Formula_134_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((`File Name` IS NULL) OR (CAST(TRIM(`File Name`) AS string) = ''))
          THEN '4PL Inventory Report'
        ELSE `File Name`
      END
    ) AS string) AS `File Name`,
    * EXCEPT (`file name`)
  
  FROM AppendFields_140 AS in0

),

PortfolioComposerRender_57 AS (

  {{ prophecy_basics.ToDo('Component type: Render is not supported.') }}

)

SELECT *

FROM PortfolioComposerRender_57
