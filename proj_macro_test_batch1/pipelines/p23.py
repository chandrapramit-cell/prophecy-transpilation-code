from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p23", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    pipeline_1 = Process(
        name = "Pipeline_1",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "p1",
          parameterSet = "",
          parameters = {}
        )
    )
    excel_file_list = Process(
        name = "excel_file_list",
        properties = Dataset(table = Dataset.DBTSource(name = "excel_file_list", sourceType = "Seed")),
        input_ports = None
    )
    p23__excel_data_combined = Process(
        name = "p23__excel_data_combined",
        properties = ModelTransform(modelName = "p23__excel_data_combined")
    )
    p23__parse_excel_path_1 = Process(
        name = "p23__parse_excel_path_1",
        properties = ModelTransform(modelName = "p23__parse_excel_path_1")
    )
    read_excel_sheets = Process(
        name = "read_excel_sheets",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          outputMode = "unionDatasetByName",
          filePathColumnName = "source_file",
          fileType = "fileType_XLSX",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "databricks_default",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True,
        comment = "Ingests and consolidates Excel files from Databricks, unifying sheets by name and tagging each row with its source file and sheet for downstream analysis."
    )
    read_excel_sheets >> p23__excel_data_combined
    p23__excel_data_combined >> pipeline_1
    excel_file_list >> p23__parse_excel_path_1
    p23__parse_excel_path_1 >> read_excel_sheets
