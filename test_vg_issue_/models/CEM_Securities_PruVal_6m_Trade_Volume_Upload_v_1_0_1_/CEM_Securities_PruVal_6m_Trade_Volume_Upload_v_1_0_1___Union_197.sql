{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH PortfolioComposerTable_230 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_', 
      'PortfolioComposerTable_230'
    )
  }}

),

PortfolioComposerTable_194 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_', 
      'PortfolioComposerTable_194'
    )
  }}

),

PortfolioComposerTable_193 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_', 
      'PortfolioComposerTable_193'
    )
  }}

),

PortfolioComposerTable_192 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_', 
      'PortfolioComposerTable_192'
    )
  }}

),

Union_197 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'PortfolioComposerTable_194', 
        'PortfolioComposerTable_193', 
        'PortfolioComposerTable_192', 
        'PortfolioComposerTable_230'
      ], 
      [
        '[{"name": "Trade Date", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "Table", "dataType": "String"}]', 
        '[{"name": "Counterparty Name", "dataType": "String"}, {"name": "Y/N?", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "Table", "dataType": "String"}]', 
        '[{"name": "Instrument Description", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "Table", "dataType": "String"}]', 
        '[{"name": "Currency", "dataType": "String"}, {"name": "variableDate", "dataType": "Date"}, {"name": "Name", "dataType": "String"}, {"name": "Table", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_197
