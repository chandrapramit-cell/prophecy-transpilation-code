{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH R_64_12 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('TS_Factory_Sample1', 'R_64_12', 'out0') }}

),

DynamicRename_82_12 AS (

  {#VisualGroup: ToolsUsed#}
  {{
    prophecy_basics.MultiColumnRename(
      ['R_64_12'], 
      [
        'Q_COEFFICIENTS_FOR_SEASONAL_MODELS_1', 
        'Q_COEFFICIENTS_1', 
        'P_VALUE', 
        'ME', 
        'MPE', 
        'INTERCEPT', 
        'RMSE', 
        'MAE', 
        'BIC', 
        'DEGREES_OF_FREEDOM', 
        'D_SEASONAL', 
        'AICC', 
        'MAPE', 
        'VARIABLEGROUP', 
        'Q', 
        'MASE', 
        'P', 
        'X_SQUARED_STATISTIC_LJUNG_BOX', 
        'P_COEFFICIENTS_1', 
        'SIGMA_2', 
        'ACF1', 
        'P_COEFFICIENTS_FOR_SEASONAL_MODELS_1', 
        'Q_SEASONAL', 
        'LOGLIK', 
        'P_SEASONAL', 
        'AIC', 
        'D'
      ], 
      'advancedRename', 
      [
        'P_COEFFICIENTS_1', 
        'ME', 
        'MPE', 
        'RMSE', 
        'LOGLIK', 
        'SIGMA_2', 
        'MAE', 
        'BIC', 
        'Q_COEFFICIENTS_1', 
        'X_SQUARED_STATISTIC_LJUNG_BOX', 
        'AICC', 
        'MAPE', 
        'P_VALUE', 
        'INTERCEPT', 
        'Q', 
        'P_COEFFICIENTS_FOR_SEASONAL_MODELS_1', 
        'MASE', 
        'Q_COEFFICIENTS_FOR_SEASONAL_MODELS_1', 
        'P', 
        'D_SEASONAL', 
        'DEGREES_OF_FREEDOM', 
        'Q_SEASONAL', 
        'P_SEASONAL', 
        'AIC', 
        'ACF1', 
        'D', 
        'VARIABLEGROUP'
      ], 
      'Suffix', 
      '', 
      "regexp_replace(column_name, '_', ' ')"
    )
  }}

),

DynamicRename_83_12_before AS (

  {#VisualGroup: ToolsUsed#}
  SELECT 
    CAST(NULL AS STRING) AS SIGMA__2,
    *
  
  FROM DynamicRename_82_12 AS in0

),

DynamicRename_83_12 AS (

  {#VisualGroup: ToolsUsed#}
  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_83_12_before'], 
      [
        'P COEFFICIENTS 1', 
        'SIGMA__2', 
        'SIGMA 2', 
        'Q COEFFICIENTS 1', 
        'P COEFFICIENTS FOR SEASONAL MODELS 1', 
        'Q COEFFICIENTS FOR SEASONAL MODELS 1'
      ], 
      'advancedRename', 
      [
        'SIGMA__2', 
        'SIGMA 2', 
        'X SQUARED STATISTIC LJUNG BOX', 
        'ME', 
        'MPE', 
        'RMSE', 
        'LOGLIK', 
        'MAE', 
        'BIC', 
        'Q COEFFICIENTS 1', 
        'AICC', 
        'MAPE', 
        'P SEASONAL', 
        'D SEASONAL', 
        'INTERCEPT', 
        'Q', 
        'P VALUE', 
        'MASE', 
        'Q SEASONAL', 
        'P', 
        'DEGREES OF FREEDOM', 
        'Q COEFFICIENTS FOR SEASONAL MODELS 1', 
        'P COEFFICIENTS FOR SEASONAL MODELS 1', 
        'AIC', 
        'P COEFFICIENTS 1', 
        'ACF1', 
        'D', 
        'VARIABLEGROUP'
      ], 
      'Suffix', 
      '', 
      "regexp_replace(column_name, '.', '^')"
    )
  }}

),

DynamicRename_83_12_after AS (

  {#VisualGroup: ToolsUsed#}
  SELECT 
    CAST(NULL AS STRING) AS SIGMA__2,
    *
  
  FROM DynamicRename_83_12 AS in0

),

DynamicRename_84_12_before AS (

  {#VisualGroup: ModuleDescription#}
  SELECT 
    CAST(NULL AS STRING) AS P__COEFFICIENTS__1,
    CAST(NULL AS STRING) AS Q__COEFFICIENTS__1,
    CAST(NULL AS STRING) AS Q__COEFFICIENTS__2,
    CAST(NULL AS STRING) AS P__COEFFICIENTS__FOR__SEASONAL__MODELS__1,
    CAST(NULL AS STRING) AS Q__COEFFICIENTS__FOR__SEASONAL__MODELS__1,
    CAST(NULL AS STRING) AS COEFFICIENTS__OF__COVARIATES__1,
    CAST(NULL AS STRING) AS COEFFICIENTS__OF__COVARIATES__2,
    *
  
  FROM DynamicRename_83_12_after AS in0

),

DynamicRename_84_12 AS (

  {#VisualGroup: InterfaceTools#}
  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_84_12_before'], 
      [
        'Q SEASONAL', 
        'ME', 
        'MPE', 
        'INTERCEPT', 
        'RMSE', 
        'Q__COEFFICIENTS__2', 
        'P VALUE', 
        'MAE', 
        'P__COEFFICIENTS__FOR__SEASONAL__MODELS__1', 
        'BIC', 
        'D SEASONAL', 
        'P__COEFFICIENTS__1', 
        'COEFFICIENTS__OF__COVARIATES__2', 
        'Q__COEFFICIENTS__FOR__SEASONAL__MODELS__1', 
        'P SEASONAL', 
        'P COEFFICIENTS 1', 
        'AICC', 
        'MAPE', 
        'SIGMA__2', 
        'VARIABLEGROUP', 
        'Q', 
        'DEGREES OF FREEDOM', 
        'MASE', 
        'P', 
        'COEFFICIENTS__OF__COVARIATES__1', 
        'SIGMA 2', 
        'X SQUARED STATISTIC LJUNG BOX', 
        'Q COEFFICIENTS 1', 
        'P COEFFICIENTS FOR SEASONAL MODELS 1', 
        'Q COEFFICIENTS FOR SEASONAL MODELS 1', 
        'Q__COEFFICIENTS__1', 
        'ACF1', 
        'LOGLIK', 
        'AIC', 
        'D'
      ], 
      'advancedRename', 
      [
        'P__COEFFICIENTS__1', 
        'Q__COEFFICIENTS__1', 
        'Q__COEFFICIENTS__2', 
        'P__COEFFICIENTS__FOR__SEASONAL__MODELS__1', 
        'Q__COEFFICIENTS__FOR__SEASONAL__MODELS__1', 
        'COEFFICIENTS__OF__COVARIATES__1', 
        'COEFFICIENTS__OF__COVARIATES__2', 
        'SIGMA__2', 
        'SIGMA 2', 
        'X SQUARED STATISTIC LJUNG BOX', 
        'ME', 
        'MPE', 
        'RMSE', 
        'LOGLIK', 
        'MAE', 
        'BIC', 
        'Q COEFFICIENTS 1', 
        'AICC', 
        'MAPE', 
        'P SEASONAL', 
        'D SEASONAL', 
        'INTERCEPT', 
        'Q', 
        'P VALUE', 
        'MASE', 
        'Q SEASONAL', 
        'P', 
        'DEGREES OF FREEDOM', 
        'Q COEFFICIENTS FOR SEASONAL MODELS 1', 
        'P COEFFICIENTS FOR SEASONAL MODELS 1', 
        'AIC', 
        'P COEFFICIENTS 1', 
        'ACF1', 
        'D', 
        'VARIABLEGROUP'
      ], 
      'Suffix', 
      '', 
      "regexp_replace(column_name, '.', ' ')"
    )
  }}

),

DynamicRename_84_12_after AS (

  {#VisualGroup: InterfaceTools#}
  SELECT 
    CAST(NULL AS STRING) AS P__COEFFICIENTS__1,
    CAST(NULL AS STRING) AS Q__COEFFICIENTS__1,
    CAST(NULL AS STRING) AS Q__COEFFICIENTS__2,
    CAST(NULL AS STRING) AS P__COEFFICIENTS__FOR__SEASONAL__MODELS__1,
    CAST(NULL AS STRING) AS Q__COEFFICIENTS__FOR__SEASONAL__MODELS__1,
    CAST(NULL AS STRING) AS COEFFICIENTS__OF__COVARIATES__1,
    CAST(NULL AS STRING) AS COEFFICIENTS__OF__COVARIATES__2,
    *
  
  FROM DynamicRename_84_12 AS in0

)

SELECT *

FROM DynamicRename_84_12_after
