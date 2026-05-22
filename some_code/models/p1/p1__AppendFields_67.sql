{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH User_Repository_30 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'User_Repository_30') }}

),

Formula_37_0 AS (

  SELECT 
    CAST(UPPER(SID) AS string) AS SID,
    * EXCEPT (`sid`)
  
  FROM User_Repository_30 AS in0

),

DynamicInput_5 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'DynamicInput_5') }}

),

Formula_36_0 AS (

  SELECT 
    CAST(UPPER((SUBSTRING(NAME, ((((LOCATE('\\', NAME)) - 1) + 1) + 1), (LENGTH(NAME))))) AS string) AS NAME,
    * EXCEPT (`name`)
  
  FROM DynamicInput_5 AS in0

),

Join_31_inner AS (

  SELECT 
    in1.ProjectName AS ProjectName,
    in0.NAME AS NAME,
    in1.ARTProject_ID AS ARTProject_ID,
    in1.Remark AS Remark,
    in1.Run_Access AS Run_Access,
    in1.SID AS SID
  
  FROM Formula_36_0 AS in0
  INNER JOIN Formula_37_0 AS in1
     ON (in0.NAME = in1.SID)

),

Formula_27_to_Formula_39_0 AS (

  SELECT 
    CAST(ARTProject_ID AS string) AS ARTProject_ID,
    * EXCEPT (`artproject_id`)
  
  FROM Join_31_inner AS in0

),

Formula_27_to_Formula_39_1 AS (

  SELECT 
    CAST((coalesce((CONTAINS(LOWER(ProjectName), LOWER(ARTProject_ID))), FALSE)) AS INTEGER) AS ValidateWorkflow,
    *
  
  FROM Formula_27_to_Formula_39_0 AS in0

),

Filter_34 AS (

  SELECT * 
  
  FROM Formula_27_to_Formula_39_1 AS in0
  
  WHERE ((Run_Access = 'Yes') AND (ValidateWorkflow = -1))

),

CountRecords_32 AS (

  SELECT COUNT('1') AS `Count`
  
  FROM Filter_34 AS in0

),

AppendFields_33 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Filter_34 AS in0
  INNER JOIN CountRecords_32 AS in1
     ON TRUE

),

Config_46_xlsx__69 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'Config_46_xlsx__69') }}

),

Formula_73_0 AS (

  SELECT 
    CAST((CONCAT(Config_File, '|||xxx')) AS string) AS Config_File,
    CAST((CONCAT(MasterFile, '|||xxx')) AS string) AS MasterFile,
    * EXCEPT (`config_file`, `masterfile`)
  
  FROM Config_46_xlsx__69 AS in0

),

AppendFields_67 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Formula_73_0 AS in0
  INNER JOIN AppendFields_33 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_67
