{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_163_0 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__Formula_163_0')}}

),

PortfolioComposerTable_167 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

PortfolioComposerText_168 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

AlteryxSelect_169 AS (

  SELECT 
    `Same File Types` AS `Same File Types`,
    `Actual Data Type` AS `Actual Data Type`,
    `Expected File Format` AS `Expected File Format`,
    `Expected Data Type` AS `Expected Data Type`,
    `Actual File Format` AS `Actual File Format`,
    Text AS Text,
    Table AS `File Type Table`
  
  FROM PortfolioComposerText_168 AS in0

),

PortfolioComposerText_103 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

JoinMultiple_86_in5 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_5`
  
  FROM PortfolioComposerText_103

),

AlteryxSelect_25 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__AlteryxSelect_25')}}

),

Join_72_inner AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__Join_72_inner')}}

),

AlteryxSelect_71 AS (

  SELECT * 
  
  FROM Join_72_inner AS in0

),

PortfolioComposerTable_73 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

PortfolioComposerText_74 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

AlteryxSelect_84 AS (

  SELECT Text AS Text
  
  FROM PortfolioComposerText_74 AS in0

),

PortfolioComposerTable_26 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

PortfolioComposerText_62 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

Query__Sheet1___144 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DQ_Macro', 'Query__Sheet1___144') }}

),

CountRecords_134 AS (

  SELECT COUNT(*) AS `Count`
  
  FROM Query__Sheet1___144 AS in0

),

PortfolioComposerTable_135 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

JoinMultiple_86_in1 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_1`
  
  FROM AlteryxSelect_84

),

PortfolioComposerText_136 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

AlteryxSelect_137 AS (

  SELECT 
    Text AS Text,
    Table AS `Count Table`
  
  FROM PortfolioComposerText_136 AS in0

),

JoinMultiple_86_in6 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_6`
  
  FROM AlteryxSelect_137

),

AlteryxSelect_19 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__AlteryxSelect_19')}}

),

PortfolioComposerTable_17 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

PortfolioComposerText_54 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

AlteryxSelect_82 AS (

  SELECT Text AS Text
  
  FROM PortfolioComposerText_54 AS in0

),

JoinMultiple_86_in4 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_4`
  
  FROM AlteryxSelect_82

),

AlteryxSelect_20 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__AlteryxSelect_20')}}

),

PortfolioComposerTable_21 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

PortfolioComposerText_56 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

AlteryxSelect_83 AS (

  SELECT Text AS Text
  
  FROM PortfolioComposerText_56 AS in0

),

JoinMultiple_86_in0 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_0`
  
  FROM AlteryxSelect_83

),

Filter_24 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__Filter_24')}}

),

AlteryxSelect_38 AS (

  SELECT 
    NAME AS `Expected Fields`,
    VARIABLETYPE AS `Expected Field Types`,
    RIGHT_NAME AS `Actual Fields`,
    RIGHT_TYPE AS `Actual Field Types`
  
  FROM Filter_24 AS in0

),

PortfolioComposerTable_39 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

PortfolioComposerText_61 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

AlteryxSelect_80 AS (

  SELECT Text AS Text
  
  FROM PortfolioComposerText_61 AS in0

),

JoinMultiple_86_in3 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_3`
  
  FROM AlteryxSelect_80

),

AlteryxSelect_81 AS (

  SELECT Text AS Text
  
  FROM PortfolioComposerText_62 AS in0

),

JoinMultiple_86_in2 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_2`
  
  FROM AlteryxSelect_81

),

JoinMultiple_86_in7 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin_7`
  
  FROM AlteryxSelect_169

),

JoinMultiple_86 AS (

  SELECT 
    in7.`File Type Table` AS `File Type Table`,
    in7.`Same File Types` AS `Same File Types`,
    in6.`Count Table` AS `Count Table`,
    in0.Text AS Text,
    in7.`Expected File Format` AS `Expected File Format`,
    in7.`Actual File Format` AS `Actual File Format`,
    in7.`Actual Data Type` AS `Actual Data Type`,
    in7.`Expected Data Type` AS `Expected Data Type`
  
  FROM JoinMultiple_86_in0 AS in0
  FULL JOIN JoinMultiple_86_in1 AS in1
     ON (in0.recordPositionForJoin_0 = in1.recordPositionForJoin_1)
  FULL JOIN JoinMultiple_86_in2 AS in2
     ON (coalesce(in0.recordPositionForJoin_0, in1.recordPositionForJoin_1) = in2.recordPositionForJoin_2)
  FULL JOIN JoinMultiple_86_in3 AS in3
     ON (coalesce(in0.recordPositionForJoin_0, in1.recordPositionForJoin_1, in2.recordPositionForJoin_2) = in3.recordPositionForJoin_3)
  FULL JOIN JoinMultiple_86_in4 AS in4
     ON (
      coalesce(
        in0.recordPositionForJoin_0, 
        in1.recordPositionForJoin_1, 
        in2.recordPositionForJoin_2, 
        in3.recordPositionForJoin_3) = in4.recordPositionForJoin_4
    )
  FULL JOIN JoinMultiple_86_in5 AS in5
     ON (
      coalesce(
        in0.recordPositionForJoin_0, 
        in1.recordPositionForJoin_1, 
        in2.recordPositionForJoin_2, 
        in3.recordPositionForJoin_3, 
        in4.recordPositionForJoin_4) = in5.recordPositionForJoin_5
    )
  FULL JOIN JoinMultiple_86_in6 AS in6
     ON (
      coalesce(
        in0.recordPositionForJoin_0, 
        in1.recordPositionForJoin_1, 
        in2.recordPositionForJoin_2, 
        in3.recordPositionForJoin_3, 
        in4.recordPositionForJoin_4, 
        in5.recordPositionForJoin_5) = in6.recordPositionForJoin_6
    )
  FULL JOIN JoinMultiple_86_in7 AS in7
     ON (
      coalesce(
        in0.recordPositionForJoin_0, 
        in1.recordPositionForJoin_1, 
        in2.recordPositionForJoin_2, 
        in3.recordPositionForJoin_3, 
        in4.recordPositionForJoin_4, 
        in5.recordPositionForJoin_5, 
        in6.recordPositionForJoin_6) = in7.recordPositionForJoin_7
    )

),

ReportHeader_88 AS (

  {{ prophecy_basics.ToDo('Component type: ReportHeader is not supported.') }}

),

PortfolioComposerLayout_94 AS (

  {{ prophecy_basics.ToDo('Component type: Layout is not supported.') }}

),

AlteryxSelect_112 AS (

  SELECT 
    Layout AS Layout,
    `File Type Table` AS `File Type Table`
  
  FROM PortfolioComposerLayout_94 AS in0

)

SELECT *

FROM AlteryxSelect_112
