{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH PortfolioComposerTable_64 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume', 'PortfolioComposerTable_64') }}

),

PortfolioComposerTable_65 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume', 'PortfolioComposerTable_65') }}

),

PortfolioComposerTable_66 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume', 'PortfolioComposerTable_66') }}

),

PortfolioComposerTable_72 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume', 'PortfolioComposerTable_72') }}

),

Union_62 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'PortfolioComposerTable_64', 
        'PortfolioComposerTable_65', 
        'PortfolioComposerTable_66', 
        'PortfolioComposerTable_72'
      ], 
      [
        '[{"name": "Trade Date", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "Table", "dataType": "String"}]', 
        '[{"name": "Counterparty Name", "dataType": "String"}, {"name": "Y/N?", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "Table", "dataType": "String"}]', 
        '[{"name": "Instrument Description", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "Table", "dataType": "String"}]', 
        '[{"name": "variableDate", "dataType": "Date"}, {"name": "Currency", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "Table", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_62
