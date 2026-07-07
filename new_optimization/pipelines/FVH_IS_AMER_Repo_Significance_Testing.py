from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "FVH_IS_AMER_Repo_Significance_Testing",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'FVH_IS_AMER_Repo_Significance_Testing'")
)

with Pipeline(args) as pipeline:
    checks_xlsx_que_127 = Process(
        name = "Checks_xlsx_Que_127",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "Included SS CurveType Risk Type"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    checks_xlsx_que_141 = Process(
        name = "Checks_xlsx_Que_141",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "Notional Ratio Missing"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    checks_xlsx_que_149 = Process(
        name = "Checks_xlsx_Que_149",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "MTM Mismatch"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    checks_xlsx_que_151 = Process(
        name = "Checks_xlsx_Que_151",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "Significance Test Missed Trades"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    checks_xlsx_que_44 = Process(
        name = "Checks_xlsx_Que_44",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "VA Curve to Map"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    checks_xlsx_que_62 = Process(
        name = "Checks_xlsx_Que_62",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "Missing MTM"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    checks_xlsx_que_70 = Process(
        name = "Checks_xlsx_Que_70",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "TradeCurveList"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    dynamicinput_205 = Process(
        name = "DynamicInput_205",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "FileName",
          passFieldsToOutput = [],
          fileType = "fileType_CSV",
          sqlQuery = "",
          csvFilePathColumn = "OfficialTRisk_NA - Repos DL",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_206 = Process(
        name = "DynamicInput_206",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "FileName",
          passFieldsToOutput = [],
          fileType = "fileType_CSV",
          sqlQuery = "",
          csvFilePathColumn = "OfficialTRisk_FIF_EM_DL",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_207 = Process(
        name = "DynamicInput_207",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_208 = Process(
        name = "DynamicInput_208",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_209 = Process(
        name = "DynamicInput_209",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_211 = Process(
        name = "DynamicInput_211",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_212 = Process(
        name = "DynamicInput_212",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_213 = Process(
        name = "DynamicInput_213",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_214 = Process(
        name = "DynamicInput_214",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_215 = Process(
        name = "DynamicInput_215",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_217 = Process(
        name = "DynamicInput_217",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "FileName",
          passFieldsToOutput = [],
          fileType = "fileType_CSV",
          sqlQuery = "",
          csvFilePathColumn = "Athena FVO",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_218 = Process(
        name = "DynamicInput_218",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_219 = Process(
        name = "DynamicInput_219",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_220 = Process(
        name = "DynamicInput_220",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_221 = Process(
        name = "DynamicInput_221",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_262 = Process(
        name = "DynamicInput_262",
        properties = DynamicInput(
          xlsxFileIntegration = "databricks",
          replaceSpecificString = [],
          tableIntegration = "databricks",
          outputMode = "unionDatasetByName",
          tableConnector = "transpiled_connection",
          filePathColumnName = "source_file",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "",
          sheetNameColumnName = "source_sheet",
          header = True,
          fileConnector = "transpiled_connection",
          readOptions = "dynamicReadFiles",
          xlsxSheetColumn = "sheet_name",
          xlsxFilePathColumn = "file_path"
        ),
        is_custom_output_schema = True
    )
    fvh_is_amer_repo_significance_testing__alteryxselect_42 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AlteryxSelect_42",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AlteryxSelect_42")
    )
    fvh_is_amer_repo_significance_testing__appendfields_223 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_223",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_223"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_227 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_227",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_227"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_228 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_228",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_228"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_230 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_230",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_230"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_232 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_232",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_232"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_234 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_234",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_234"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_237 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_237",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_237"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_239 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_239",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_239"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_241 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_241",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_241"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_243 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_243",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_243"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_245 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_245",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_245"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_247 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_247",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_247"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_249 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_249",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_249"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_251 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_251",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_251"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_253 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_253",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_253"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_255 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_255",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_255"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing__appendfields_256 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_256",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__AppendFields_256"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_207_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_207_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_207_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_208_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_208_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_208_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_209_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_209_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_209_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_211_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_211_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_211_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_212_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_212_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_212_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_213_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_213_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_213_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_214_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_214_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_214_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_215_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_215_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_215_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_218_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_218_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_218_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_219_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_219_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_219_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_220_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_220_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_220_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_221_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_221_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_221_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_262_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_262_readListPathSheet_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__DynamicInput_262_readListPathSheet_0")
    )
    fvh_is_amer_repo_significance_testing__filter_264 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Filter_264",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Filter_264")
    )
    fvh_is_amer_repo_significance_testing__filter_264_reject = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Filter_264_reject",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Filter_264_reject")
    )
    fvh_is_amer_repo_significance_testing__formula_170_to_formula_59_2 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Formula_170_to_Formula_59_2",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Formula_170_to_Formula_59_2")
    )
    fvh_is_amer_repo_significance_testing__formula_204_1 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Formula_204_1",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Formula_204_1")
    )
    fvh_is_amer_repo_significance_testing__formula_235_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Formula_235_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Formula_235_0")
    )
    fvh_is_amer_repo_significance_testing__formula_31_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Formula_31_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Formula_31_0"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__formula_56_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Formula_56_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Formula_56_0")
    )
    fvh_is_amer_repo_significance_testing__formula_57_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Formula_57_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Formula_57_0"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing__formula_8_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Formula_8_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Formula_8_0"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__formula_90_1 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Formula_90_1",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Formula_90_1"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing__join_140_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Join_140_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Join_140_inner"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__join_142_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Join_142_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Join_142_inner"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing__join_150_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Join_150_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Join_150_inner"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__join_168_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Join_168_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Join_168_inner"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    fvh_is_amer_repo_significance_testing__join_17_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Join_17_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Join_17_inner"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__join_190_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Join_190_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Join_190_inner"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__join_190_left = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Join_190_left",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Join_190_left"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__join_72_inner_formula_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Join_72_inner_formula_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Join_72_inner_formula_0")
    )
    fvh_is_amer_repo_significance_testing__summarize_139 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Summarize_139",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Summarize_139")
    )
    fvh_is_amer_repo_significance_testing__summarize_175 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Summarize_175",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Summarize_175")
    )
    fvh_is_amer_repo_significance_testing__summarize_60 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Summarize_60",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Summarize_60")
    )
    fvh_is_amer_repo_significance_testing__textinput_51_cast = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__TextInput_51_cast",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__TextInput_51_cast")
    )
    fvh_is_amer_repo_significance_testing__union_191_postrename = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Union_191_postRename",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Union_191_postRename"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    fvh_is_amer_repo_significance_testing__union_197_postrename = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Union_197_postRename",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Union_197_postRename"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    fvh_is_amer_repo_significance_testing__union_36_postrename = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Union_36_postRename",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Union_36_postRename"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing__union_41_postrename = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Union_41_postRename",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Union_41_postRename"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing__unique_65 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing__Unique_65",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing__Unique_65"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    filteredrisk_xl_15 = Process(
        name = "FilteredRisk_xl_15",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\FilteredRisk.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "Adjusted Curve Risk"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    filteredrisk_xl_187 = Process(
        name = "FilteredRisk_xl_187",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\FilteredRisk.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "Check Totem Tested Curve"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    filteredrisk_xl_37 = Process(
        name = "FilteredRisk_xl_37",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\FilteredRisk.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "Special ISIN Adjusted Risk"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    mtminreporting__164 = Process(
        name = "MTMinreporting__164",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\MTM in reporting.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    missingthreshol_53 = Process(
        name = "MissingThreshol_53",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Missing Threshold.csv"
          ),
          format = DatabricksVolumeTarget.CsvWriteFormat(),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    reporting_xlsx__93 = Process(
        name = "Reporting_xlsx__93",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Reporting.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "Reporting"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    result_xlsx_que_29 = Process(
        name = "Result_xlsx_Que_29",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Result.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    sigtestoutput_x_174 = Process(
        name = "Sigtestoutput_x_174",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Sig test output.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    textinput_166 = Process(
        name = "TextInput_166",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_FVH_IS_AMER_Repo_Significance_Testing_166", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_203 = Process(
        name = "TextInput_203",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_FVH_IS_AMER_Repo_Significance_Testing_203", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_51 = Process(
        name = "TextInput_51",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_FVH_IS_AMER_Repo_Significance_Testing_51", sourceType = "Seed")
        ),
        input_ports = None
    )
    trades_xlsx_que_189 = Process(
        name = "Trades_xlsx_Que_189",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Trades.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(sheetName = "Backtested Trades"),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    mtm_xlsx_query__163 = Process(
        name = "mtm_xlsx_Query__163",
        properties = DatabricksVolumeTarget(
          connector = "databricks_default",
          properties = DatabricksVolumeTarget.DatabricksVolumeTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\mtm.xlsx"
          ),
          format = DatabricksVolumeTarget.XLSXWriteFormat(),
          compression = DatabricksVolumeTarget.Compression(kind = "uncompressed")
        ),
        output_ports = None
    )
    fvh_is_amer_repo_significance_testing__appendfields_237 >> filteredrisk_xl_187
    fvh_is_amer_repo_significance_testing__appendfields_227 >> filteredrisk_xl_37
    textinput_166 >> fvh_is_amer_repo_significance_testing__join_168_inner._in(1)
    (
        fvh_is_amer_repo_significance_testing__formula_90_1._out(0)
        >> [fvh_is_amer_repo_significance_testing__join_142_inner._in(2),
              fvh_is_amer_repo_significance_testing__appendfields_251._in(1)]
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_219_readlistpathsheet_0 >> dynamicinput_219
    dynamicinput_218 >> fvh_is_amer_repo_significance_testing__unique_65._in(2)
    dynamicinput_208 >> fvh_is_amer_repo_significance_testing__unique_65._in(0)
    (
        fvh_is_amer_repo_significance_testing__union_41_postrename._out(0)
        >> [fvh_is_amer_repo_significance_testing__appendfields_230._in(1),
              fvh_is_amer_repo_significance_testing__formula_57_0._in(1)]
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_213_readlistpathsheet_0 >> dynamicinput_213
    (
        fvh_is_amer_repo_significance_testing__textinput_51_cast._out(0)
        >> [fvh_is_amer_repo_significance_testing__formula_57_0._in(2),
              fvh_is_amer_repo_significance_testing__union_191_postrename._in(4)]
    )
    dynamicinput_221 >> fvh_is_amer_repo_significance_testing__join_168_inner._in(0)
    textinput_51 >> fvh_is_amer_repo_significance_testing__textinput_51_cast
    dynamicinput_219 >> fvh_is_amer_repo_significance_testing__unique_65._in(3)
    fvh_is_amer_repo_significance_testing__appendfields_239 >> trades_xlsx_que_189
    (
        fvh_is_amer_repo_significance_testing__union_191_postrename._out(0)
        >> [fvh_is_amer_repo_significance_testing__appendfields_239._in(0),
              fvh_is_amer_repo_significance_testing__join_190_left._in(1),
              fvh_is_amer_repo_significance_testing__join_190_inner._in(1)]
    )
    dynamicinput_213 >> fvh_is_amer_repo_significance_testing__formula_56_0
    (
        dynamicinput_262._out(0)
        >> [fvh_is_amer_repo_significance_testing__union_197_postrename._in(2),
              fvh_is_amer_repo_significance_testing__union_197_postrename._in(3)]
    )
    fvh_is_amer_repo_significance_testing__appendfields_245 >> sigtestoutput_x_174
    (
        fvh_is_amer_repo_significance_testing__unique_65._out(0)
        >> [fvh_is_amer_repo_significance_testing__appendfields_247._in(1),
              fvh_is_amer_repo_significance_testing__union_197_postrename._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_234._in(0)]
    )
    (
        fvh_is_amer_repo_significance_testing__formula_56_0._out(0)
        >> [fvh_is_amer_repo_significance_testing__appendfields_232._in(0),
              fvh_is_amer_repo_significance_testing__join_168_inner._in(2)]
    )
    fvh_is_amer_repo_significance_testing__appendfields_232 >> missingthreshol_53
    (
        fvh_is_amer_repo_significance_testing__alteryxselect_42._out(0)
        >> [fvh_is_amer_repo_significance_testing__union_41_postrename._in(1),
              fvh_is_amer_repo_significance_testing__union_36_postrename._in(0),
              fvh_is_amer_repo_significance_testing__union_36_postrename._in(1)]
    )
    (
        dynamicinput_214._out(0)
        >> [fvh_is_amer_repo_significance_testing__union_191_postrename._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_237._in(1)]
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_207_readlistpathsheet_0 >> dynamicinput_207
    dynamicinput_220 >> fvh_is_amer_repo_significance_testing__union_191_postrename._in(2)
    fvh_is_amer_repo_significance_testing__appendfields_234 >> mtm_xlsx_query__163
    fvh_is_amer_repo_significance_testing__dynamicinput_262_readlistpathsheet_0 >> dynamicinput_262
    fvh_is_amer_repo_significance_testing__dynamicinput_211_readlistpathsheet_0 >> dynamicinput_211
    fvh_is_amer_repo_significance_testing__dynamicinput_208_readlistpathsheet_0 >> dynamicinput_208
    fvh_is_amer_repo_significance_testing__appendfields_247 >> checks_xlsx_que_62
    fvh_is_amer_repo_significance_testing__appendfields_253 >> reporting_xlsx__93
    (
        fvh_is_amer_repo_significance_testing__formula_235_0._out(0)
        >> [fvh_is_amer_repo_significance_testing__appendfields_230._in(2),
              fvh_is_amer_repo_significance_testing__appendfields_232._in(2),
              fvh_is_amer_repo_significance_testing__appendfields_247._in(2),
              fvh_is_amer_repo_significance_testing__appendfields_255._in(2),
              fvh_is_amer_repo_significance_testing__appendfields_249._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_253._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_251._in(0),
              fvh_is_amer_repo_significance_testing__appendfields_256._in(2),
              fvh_is_amer_repo_significance_testing__appendfields_245._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_239._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_241._in(2),
              fvh_is_amer_repo_significance_testing__appendfields_243._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_228._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_237._in(2),
              fvh_is_amer_repo_significance_testing__appendfields_227._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_223._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_234._in(1),
              fvh_is_amer_repo_significance_testing__dynamicinput_262_readlistpathsheet_0._in(0)]
    )
    dynamicinput_211 >> fvh_is_amer_repo_significance_testing__alteryxselect_42
    (
        fvh_is_amer_repo_significance_testing__formula_31_0._out(0)
        >> [fvh_is_amer_repo_significance_testing__union_36_postrename._in(2),
              fvh_is_amer_repo_significance_testing__appendfields_223._in(0)]
    )
    dynamicinput_217 >> fvh_is_amer_repo_significance_testing__unique_65._in(1)
    fvh_is_amer_repo_significance_testing__dynamicinput_214_readlistpathsheet_0 >> dynamicinput_214
    (
        fvh_is_amer_repo_significance_testing__join_72_inner_formula_0._out(0)
        >> [fvh_is_amer_repo_significance_testing__summarize_139._in(0),
              fvh_is_amer_repo_significance_testing__appendfields_243._in(0)]
    )
    fvh_is_amer_repo_significance_testing__appendfields_223 >> filteredrisk_xl_15
    (
        dynamicinput_207._out(0)
        >> [fvh_is_amer_repo_significance_testing__formula_31_0._in(0),
              fvh_is_amer_repo_significance_testing__join_17_inner._in(0)]
    )
    fvh_is_amer_repo_significance_testing__appendfields_251 >> mtminreporting__164
    (
        fvh_is_amer_repo_significance_testing__summarize_60._out(0)
        >> [fvh_is_amer_repo_significance_testing__appendfields_247._in(0),
              fvh_is_amer_repo_significance_testing__union_197_postrename._in(0)]
    )
    fvh_is_amer_repo_significance_testing__appendfields_241 >> checks_xlsx_que_141
    (
        fvh_is_amer_repo_significance_testing__join_142_inner._out(0)
        >> [fvh_is_amer_repo_significance_testing__appendfields_255._in(0),
              fvh_is_amer_repo_significance_testing__appendfields_253._in(0)]
    )
    dynamicinput_209 >> fvh_is_amer_repo_significance_testing__union_191_postrename._in(3)
    dynamicinput_206 >> fvh_is_amer_repo_significance_testing__formula_8_0._in(1)
    fvh_is_amer_repo_significance_testing__dynamicinput_215_readlistpathsheet_0 >> dynamicinput_215
    fvh_is_amer_repo_significance_testing__dynamicinput_221_readlistpathsheet_0 >> dynamicinput_221
    fvh_is_amer_repo_significance_testing__dynamicinput_218_readlistpathsheet_0 >> dynamicinput_218
    (
        fvh_is_amer_repo_significance_testing__formula_8_0._out(0)
        >> [fvh_is_amer_repo_significance_testing__formula_31_0._in(1),
              fvh_is_amer_repo_significance_testing__join_17_inner._in(1)]
    )
    (
        fvh_is_amer_repo_significance_testing__union_197_postrename._out(0)
        >> [fvh_is_amer_repo_significance_testing__appendfields_255._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_249._in(0),
              fvh_is_amer_repo_significance_testing__join_142_inner._in(0),
              fvh_is_amer_repo_significance_testing__formula_90_1._in(0),
              fvh_is_amer_repo_significance_testing__appendfields_256._in(1),
              fvh_is_amer_repo_significance_testing__join_150_inner._in(1)]
    )
    (
        fvh_is_amer_repo_significance_testing__formula_57_0._out(0)
        >> [fvh_is_amer_repo_significance_testing__appendfields_232._in(1),
              fvh_is_amer_repo_significance_testing__join_168_inner._in(3)]
    )
    dynamicinput_205 >> fvh_is_amer_repo_significance_testing__formula_8_0._in(0)
    (
        fvh_is_amer_repo_significance_testing__join_168_inner._out(0)
        >> [fvh_is_amer_repo_significance_testing__filter_264._in(0),
              fvh_is_amer_repo_significance_testing__filter_264_reject._in(0),
              fvh_is_amer_repo_significance_testing__formula_170_to_formula_59_2._in(0)]
    )
    (
        fvh_is_amer_repo_significance_testing__formula_170_to_formula_59_2._out(0)
        >> [fvh_is_amer_repo_significance_testing__join_142_inner._in(1),
              fvh_is_amer_repo_significance_testing__formula_90_1._in(2),
              fvh_is_amer_repo_significance_testing__summarize_60._in(0),
              fvh_is_amer_repo_significance_testing__appendfields_245._in(0)]
    )
    dynamicinput_212 >> fvh_is_amer_repo_significance_testing__union_41_postrename._in(0)
    (
        fvh_is_amer_repo_significance_testing__join_190_left._out(0)
        >> [fvh_is_amer_repo_significance_testing__appendfields_230._in(0),
              fvh_is_amer_repo_significance_testing__appendfields_256._in(0),
              fvh_is_amer_repo_significance_testing__join_150_inner._in(0),
              fvh_is_amer_repo_significance_testing__formula_57_0._in(0),
              fvh_is_amer_repo_significance_testing__join_72_inner_formula_0._in(0)]
    )
    textinput_203 >> fvh_is_amer_repo_significance_testing__formula_204_1
    (
        fvh_is_amer_repo_significance_testing__summarize_175._out(0)
        >> [fvh_is_amer_repo_significance_testing__union_191_postrename._in(0),
              fvh_is_amer_repo_significance_testing__appendfields_237._in(0)]
    )
    (
        fvh_is_amer_repo_significance_testing__summarize_139._out(0)
        >> [fvh_is_amer_repo_significance_testing__appendfields_241._in(0),
              fvh_is_amer_repo_significance_testing__join_140_inner._in(0)]
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_220_readlistpathsheet_0 >> dynamicinput_220
    fvh_is_amer_repo_significance_testing__dynamicinput_209_readlistpathsheet_0 >> dynamicinput_209
    fvh_is_amer_repo_significance_testing__appendfields_230 >> checks_xlsx_que_44
    fvh_is_amer_repo_significance_testing__appendfields_249 >> result_xlsx_que_29
    (
        fvh_is_amer_repo_significance_testing__formula_204_1._out(0)
        >> [dynamicinput_205._in(0), dynamicinput_206._in(0), dynamicinput_217._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_212_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_221_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_207_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_211_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_214_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_220_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_209_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_215_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_219_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_213_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_218_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing__formula_235_0._in(0),
              fvh_is_amer_repo_significance_testing__dynamicinput_208_readlistpathsheet_0._in(0)]
    )
    fvh_is_amer_repo_significance_testing__dynamicinput_212_readlistpathsheet_0 >> dynamicinput_212
    fvh_is_amer_repo_significance_testing__appendfields_228 >> checks_xlsx_que_127
    fvh_is_amer_repo_significance_testing__appendfields_256 >> checks_xlsx_que_151
    (
        dynamicinput_215._out(0)
        >> [fvh_is_amer_repo_significance_testing__formula_90_1._in(1),
              fvh_is_amer_repo_significance_testing__appendfields_241._in(1),
              fvh_is_amer_repo_significance_testing__join_140_inner._in(1)]
    )
    fvh_is_amer_repo_significance_testing__appendfields_243 >> checks_xlsx_que_70
    fvh_is_amer_repo_significance_testing__appendfields_255 >> checks_xlsx_que_149
    (
        fvh_is_amer_repo_significance_testing__union_36_postrename._out(0)
        >> [fvh_is_amer_repo_significance_testing__join_190_left._in(0),
              fvh_is_amer_repo_significance_testing__join_190_inner._in(0),
              fvh_is_amer_repo_significance_testing__appendfields_228._in(0),
              fvh_is_amer_repo_significance_testing__summarize_175._in(0),
              fvh_is_amer_repo_significance_testing__appendfields_227._in(0)]
    )
