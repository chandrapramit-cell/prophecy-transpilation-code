{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH GenerateRows_24 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('110_2015_Day_7_Main_AndrewMerrill', 'GenerateRows_24') }}

),

Sort_33 AS (

  SELECT * 
  
  FROM GenerateRows_24 AS in0
  
  ORDER BY COUNT ASC

),

RecordID_31 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM Sort_33

),

MultiRowFormula_25_row_id_0 AS (

  SELECT 
    (ROW_NUMBER() OVER ()) AS prophecy_row_id,
    *
  
  FROM RecordID_31 AS in0

),

MultiRowFormula_25_0 AS (

  SELECT 
    CAST('IF (REGEX_Match([Input 1],"\\\\d+") or REGEX_Match(REGEX_Replace([Row-1:Logic],".*-"+[Input 1]+"-(x|\\\\d+).*", "$1"),"\\\\d+")) and (REGEX_Match([Input 2],"\\\\d+") or REGEX_Match(REGEX_Replace([Row-1:Logic],".*-"+[Input 2]+"-(x|\\\\d+).*", "$1"),"\\\\d+"))
    THEN
    Replace(
      [Row-1:Logic],
      "-"+[Output]+"-x",
      "-"+[Output]+"-" + ToString(Switch([Operation],
    //DEFAULT
    BinaryXOr(ToNumber(IIF(REGEX_Match([Input 1],"\\\\d+"),[Input 1],REGEX_Replace([Row-1:Logic],".*-"+[Input 1]+"-(x|\\\\d+).*", "$1"))),
    ToNumber(IIF(REGEX_Match([Input 2],"\\\\d+"),[Input 2],REGEX_Replace([Row-1:Logic],".*-"+[Input 2]+"-(x|\\\\d+).*", "$1")))),
    
    "AND",
    BinaryAnd(ToNumber(IIF(REGEX_Match([Input 1],"\\\\d+"),[Input 1],REGEX_Replace([Row-1:Logic],".*-"+[Input 1]+"-(x|\\\\d+).*", "$1"))),
    ToNumber(IIF(REGEX_Match([Input 2],"\\\\d+"),[Input 2],REGEX_Replace([Row-1:Logic],".*-"+[Input 2]+"-(x|\\\\d+).*", "$1")))),
    
    "OR",
    BinaryOr(ToNumber(IIF(REGEX_Match([Input 1],"\\\\d+"),[Input 1],REGEX_Replace([Row-1:Logic],".*-"+[Input 1]+"-(x|\\\\d+).*", "$1"))),
    ToNumber(IIF(REGEX_Match([Input 2],"\\\\d+"),[Input 2],REGEX_Replace([Row-1:Logic],".*-"+[Input 2]+"-(x|\\\\d+).*", "$1")))), 
    
    "LSHIFT",
    BinToInt(Right(IntToBin(ToNumber(IIF(REGEX_Match([Input 1],"\\\\d+"),[Input 1],REGEX_Replace([Row-1:Logic],".*-"+[Input 1]+"-(x|\\\\d+).*", "$1")))) + PadRight("",ToNumber(IIF(REGEX_Match([Input 2],"\\\\d+"),[Input 2],REGEX_Replace([Row-1:Logic],".*-"+[Input 2]+"-(x|\\\\d+).*", "$1"))),\'0\'),16)),
    
    "RSHIFT",
    BinToInt(LEFT(PadRight("",ToNumber(IIF(REGEX_Match([Input 2],"\\\\d+"),[Input 2],REGEX_Replace([Row-1:Logic],".*-"+[Input 2]+"-(x|\\\\d+).*", "$1"))),\'0\') + PadLeft(IntToBin(ToNumber(IIF(REGEX_Match([Input 1],"\\\\d+"),[Input 1],REGEX_Replace([Row-1:Logic],".*-"+[Input 1]+"-(x|\\\\d+).*", "$1")))),16,\'0\'),16))
        ))
      )
    ELSE
    [Row-1:Logic]
    ENDIF
    
    /*ToNumber(IIF(REGEX_Match([Input 1],"\\\\d+"),[Input 1],REGEX_Replace([Row-1:Logic],".*-"+[Input 1]+"-(x|\\\\d+).*", "$1"))) +
    ToNumber(IIF(REGEX_Match([Input 2],"\\\\d+"),[Input 2],REGEX_Replace([Row-1:Logic],".*-"+[Input 2]+"-(x|\\\\d+).*", "$1")))*/' AS INT) AS Logic,
    * EXCEPT (`logic`)
  
  FROM MultiRowFormula_25_row_id_0 AS in0

),

MultiRowFormula_25_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_25_0 AS in0

)

SELECT *

FROM MultiRowFormula_25_row_id_drop_0
