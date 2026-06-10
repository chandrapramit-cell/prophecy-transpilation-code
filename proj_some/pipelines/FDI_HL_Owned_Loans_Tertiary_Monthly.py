from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "FDI_HL_Owned_Loans_Tertiary_Monthly",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_Source__User_Db_31 = "''",
      password_Source__User_Db_31 = "''",
      username_Source__User_Db_42 = "''",
      password_Source__User_Db_42 = "''",
      username_Source__User_Db_45 = "''",
      password_Source__User_Db_45 = "''",
      workflow_name = "'FDI_HL_Owned_Loans_Tertiary_Monthly'",
      User__DbConnectionFilePath = "'File:D:\\ccbnas\\config\\db_connections\\hl-alteryx-dbx-connection.indbc'"
    )
)

with Pipeline(args) as pipeline:
    fdi_hl_owned_loans_tertiary_monthly__alteryxselect_33 = Process(
        name = "FDI_HL_Owned_Loans_Tertiary_Monthly__AlteryxSelect_33",
        properties = ModelTransform(modelName = "FDI_HL_Owned_Loans_Tertiary_Monthly__AlteryxSelect_33"),
        input_ports = None
    )
    fdi_hl_owned_loans_tertiary_monthly__formula_41_0 = Process(
        name = "FDI_HL_Owned_Loans_Tertiary_Monthly__Formula_41_0",
        properties = ModelTransform(modelName = "FDI_HL_Owned_Loans_Tertiary_Monthly__Formula_41_0"),
        input_ports = None
    )
    fdi_hl_owned_loans_tertiary_monthly__macro_29 = Process(
        name = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_29",
        properties = ModelTransform(modelName = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_29")
    )
    fdi_hl_owned_loans_tertiary_monthly__macro_35 = Process(
        name = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_35",
        properties = ModelTransform(modelName = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_35")
    )
    fdi_hl_owned_loans_tertiary_monthly__macro_48 = Process(
        name = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_48",
        properties = ModelTransform(modelName = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_48")
    )
    fdi_hl_owned_loans_tertiary_monthly__macro_50 = Process(
        name = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_50",
        properties = ModelTransform(modelName = "FDI_HL_Owned_Loans_Tertiary_Monthly__Macro_50")
    )
    orchestrationsource_1 = Process(
        name = "OrchestrationSource_1",
        properties = SnowflakeStageSource(
          connector = None,
          properties = SnowflakeStageSource.SnowflakeStageSourceInternal(),
          format = SnowflakeStageSource.CsvReadFormat()
        ),
        input_ports = None
    )
    (
        fdi_hl_owned_loans_tertiary_monthly__alteryxselect_33._out(0)
        >> [fdi_hl_owned_loans_tertiary_monthly__macro_29._in(0), fdi_hl_owned_loans_tertiary_monthly__macro_35._in(0)]
    )
    (
        fdi_hl_owned_loans_tertiary_monthly__formula_41_0._out(0)
        >> [fdi_hl_owned_loans_tertiary_monthly__macro_48._in(0), fdi_hl_owned_loans_tertiary_monthly__macro_50._in(0)]
    )
