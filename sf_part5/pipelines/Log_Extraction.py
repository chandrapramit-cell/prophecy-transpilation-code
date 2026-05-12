from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Log_Extraction",
    version = 1,
    auto_layout = False,
    params = Parameters(
      FOLDER_WHERE_YOU_WANT_THE_FOUND_LOGS_TO_BE_OUTPUT_CAN_ENTER_RELATIVE_PATHS_UNC_PATHS_OR_JUST_BROWSE_TO_THE_DESIRED_FOLDER = "''",
      FOLDER_WHERE_YOUR_DESIGNER_LOGS_ARE_WRITTEN_TO_CAN_ENTER_RELATIVE_PATHS_UNC_PATHS_OR_JUST_BROWSE_TO_THE_LOG_FOLDER = "'C:\\AlteryxLogs'",
      LOG_SEARCH_START_DATE_INCLUSIVE = "'2022-11-01'",
      LOG_SEARCH_END_DATE_INCLUSIVE = "'2022-12-31'",
      MODULE_TO_SEARCH_FOR_YXMD_OR_YXWZ_CAN_ENTER_RELATIVE_PATHS_UNC_PATHS_OR_EVEN_JUST_THE_MODULE_NAME_WITH_NO_PATH = "'module_name_here.yxmd'",
      VARIABLE21_FORMULAFIELDS_FORMULAFIELDFIELDDIRECTORY_NEW_EXPRESSION = "concat(''', CASE WHEN cast(coalesce(lower(Folder_where_you_want_the_found_logs_to_be_output_can_enter_relative_paths_unc_paths_or_just_browse_to_the_desired_folder), '').like(concat('%', lower('\\'))) as boolean) THEN Folder_where_you_want_the_found_logs_to_be_output_can_enter_relative_paths_unc_paths_or_just_browse_to_the_desired_folder ELSE concat(Folder_where_you_want_the_found_logs_to_be_output_can_enter_relative_paths_unc_paths_or_just_browse_to_the_desired_folder, '\\') END, ''')",
      VARIABLE10_DIRECTORY = Expr(
        "{{ var('Folder_where_your_designer_logs_are_written_to_can_enter_relative_paths_unc_paths_or_just_browse_to_the_log_folder') }}"
      ),
      VARIABLE12_EXPRESSION = Expr(
        "((substring(LastWriteTime, 1, 10) >= {{ var('Log_search_start_date_inclusive') }}) AND (substring(LastWriteTime, 1, 10) <= {{ var('Log_search_end_date_inclusive') }}))"
      ),
      VARIABLE16_EXPRESSION = Expr(
        "(CAST(coalesce(lower(Field_1), '') LIKE concat(lower('Started running '), '%') AS BOOLEAN) AND coalesce(contains(lower(Field_1), lower({{ var('Module_to_search_for_yxmd_or_yxwz_can_enter_relative_paths_unc_paths_or_even_just_the_module_name_with_no_path') }})), false))"
      ),
      JDBCURL_PLACEHOLDER_LOG_19 = "''",
      USERNAME_PLACEHOLDER_LOG_19 = "''",
      PASSWORD_PLACEHOLDER_LOG_19 = "''",
      WORKFLOW_NAME = "'Log_Extraction'",
      QUESTION__FOLDER_BROWSE_24 = "''",
      QUESTION__FOLDER_BROWSE_26 = "''",
      QUESTION__DATE_28 = "''",
      QUESTION__DATE_30 = "''",
      QUESTION__TEXT_BOX_32 = "''"
    )
)

with Pipeline(args) as pipeline:
    directory_10 = Process(
        name = "Directory_10",
        properties = Directory(
          path = "C:\\AlteryxLogs",
          pattern = "*.log",
          integration = "sftp",
          recursive = True,
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
            "properties": {"id" : "transpiled_connection"},
            "type": "connector"
          }
        ),
        input_ports = None
    )
    directory_11_14 = Process(
        name = "Directory_11_14",
        properties = Directory(
          path = "%temp%",
          pattern = "*.*",
          integration = "sftp",
          recursive = True,
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
            "properties": {"id" : "transpiled_connection"},
            "type": "connector"
          }
        ),
        input_ports = None
    )
    dynamicinput_15 = Process(
        name = "DynamicInput_15",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "\\\\altxapwprw2\\c$\\AlteryxAppLogs\\Alteryx_Log_1607440828_1.log",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    dynamicinput_17 = Process(
        name = "DynamicInput_17",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "\\\\altxapwprw2\\c$\\AlteryxAppLogs\\Alteryx_Log_1607440828_1.log",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    jupytercode_4_14 = Process(
        name = "JupyterCode_4_14",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = ""
        ),
        is_custom_output_schema = True
    )
    log_extraction__alteryxselect_22 = Process(
        name = "Log_Extraction__AlteryxSelect_22",
        properties = ModelTransform(modelName = "Log_Extraction__AlteryxSelect_22")
    )
    log_extraction__alteryxselect_41 = Process(
        name = "Log_Extraction__AlteryxSelect_41",
        properties = ModelTransform(modelName = "Log_Extraction__AlteryxSelect_41")
    )
    log_extraction__appendfields_40 = Process(
        name = "Log_Extraction__AppendFields_40",
        properties = ModelTransform(modelName = "Log_Extraction__AppendFields_40"),
        input_ports = ["in_0", "in_1"]
    )
    log_extraction__detourend_26_14 = Process(
        name = "Log_Extraction__DetourEnd_26_14",
        properties = ModelTransform(modelName = "Log_Extraction__DetourEnd_26_14"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    log_extraction__filter_16 = Process(
        name = "Log_Extraction__Filter_16",
        properties = ModelTransform(modelName = "Log_Extraction__Filter_16")
    )
    log_extraction__filter_5_14 = Process(
        name = "Log_Extraction__Filter_5_14",
        properties = ModelTransform(modelName = "Log_Extraction__Filter_5_14")
    )
    log_extraction__filter_5_14_reject = Process(
        name = "Log_Extraction__Filter_5_14_reject",
        properties = ModelTransform(modelName = "Log_Extraction__Filter_5_14_reject")
    )
    log_extraction__folderbrowse_17_14 = Process(
        name = "Log_Extraction__FolderBrowse_17_14",
        properties = ModelTransform(modelName = "Log_Extraction__FolderBrowse_17_14"),
        input_ports = None
    )
    log_extraction__folderbrowse_24 = Process(
        name = "Log_Extraction__FolderBrowse_24",
        properties = ModelTransform(modelName = "Log_Extraction__FolderBrowse_24"),
        input_ports = None
    )
    log_extraction__folderbrowse_26 = Process(
        name = "Log_Extraction__FolderBrowse_26",
        properties = ModelTransform(modelName = "Log_Extraction__FolderBrowse_26"),
        input_ports = None
    )
    log_extraction__formula_35_14_0 = Process(
        name = "Log_Extraction__Formula_35_14_0",
        properties = ModelTransform(modelName = "Log_Extraction__Formula_35_14_0")
    )
    log_extraction__union_12_14 = Process(
        name = "Log_Extraction__Union_12_14",
        properties = ModelTransform(modelName = "Log_Extraction__Union_12_14"),
        input_ports = ["in_0", "in_1"]
    )
    placeholder_log_19 = Process(
        name = "placeholder_log_19",
        properties = DatabricksTarget(
          connector = {
            "kind": "Databricks",
            "id": "transpiled_connection",
            "properties": {
              "catalog": "transpiled_catalog",
              "clientId": "transpiled_client_id",
              "authType": "token",
              "id": "transpiled_connection",
              "schema": "transpiled_schema",
              "jdbcUrl": "transpiled_jdbc_url",
              "token": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_token_secret", "value" : "transpiled_token_secret"},
                "subKind": "text",
                "type": "secret"
              },
              "clientSecret": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_client_secret", "value" : "transpiled_client_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = DatabricksTarget.DatabricksTargetInternal(
            tableFullName = DatabricksTarget.WarehouseTableName(name = "placeholder_log_19")
          ),
          format = DatabricksTarget.DatabricksWriteFormat()
        )
    )
    log_extraction__filter_5_14 >> dynamicinput_15
    dynamicinput_15 >> log_extraction__filter_16
    jupytercode_4_14._out(0) >> [log_extraction__detourend_26_14._in(1), log_extraction__detourend_26_14._in(2)]
    dynamicinput_17 >> log_extraction__alteryxselect_41
    directory_11_14 >> log_extraction__union_12_14._in(0)
    (
        log_extraction__detourend_26_14._out(0)
        >> [log_extraction__filter_5_14._in(0), log_extraction__filter_5_14_reject._in(0)]
    )
    directory_10 >> log_extraction__formula_35_14_0
    log_extraction__filter_16 >> dynamicinput_17
    log_extraction__union_12_14 >> jupytercode_4_14
    (
        log_extraction__alteryxselect_41._out(0)
        >> [log_extraction__alteryxselect_22._in(0), log_extraction__appendfields_40._in(0),
              log_extraction__appendfields_40._in(1)]
    )
    (
        log_extraction__formula_35_14_0._out(0)
        >> [log_extraction__detourend_26_14._in(0), log_extraction__union_12_14._in(1)]
    )
    log_extraction__alteryxselect_22 >> placeholder_log_19
