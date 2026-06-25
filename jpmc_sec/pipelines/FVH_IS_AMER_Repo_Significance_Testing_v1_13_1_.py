from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1_",
    version = 1,
    auto_layout = False,
    params = Parameters(
      jdbcUrl_Reporting_xlsx__93 = "''",
      username_Reporting_xlsx__93 = "''",
      password_Reporting_xlsx__93 = "''",
      jdbcUrl_MTMinreporting__164 = "''",
      username_MTMinreporting__164 = "''",
      password_MTMinreporting__164 = "''",
      jdbcUrl_Checks_xlsx_Que_62 = "''",
      username_Checks_xlsx_Que_62 = "''",
      password_Checks_xlsx_Que_62 = "''",
      jdbcUrl_FilteredRisk_xl_15 = "''",
      username_FilteredRisk_xl_15 = "''",
      password_FilteredRisk_xl_15 = "''",
      jdbcUrl_Checks_xlsx_Que_141 = "''",
      username_Checks_xlsx_Que_141 = "''",
      password_Checks_xlsx_Que_141 = "''",
      jdbcUrl_Checks_xlsx_Que_44 = "''",
      username_Checks_xlsx_Que_44 = "''",
      password_Checks_xlsx_Que_44 = "''",
      jdbcUrl_mtm_xlsx_Query__163 = "''",
      username_mtm_xlsx_Query__163 = "''",
      password_mtm_xlsx_Query__163 = "''",
      jdbcUrl_Sigtestoutput_x_174 = "''",
      username_Sigtestoutput_x_174 = "''",
      password_Sigtestoutput_x_174 = "''",
      jdbcUrl_FilteredRisk_xl_37 = "''",
      username_FilteredRisk_xl_37 = "''",
      password_FilteredRisk_xl_37 = "''",
      jdbcUrl_Checks_xlsx_Que_127 = "''",
      username_Checks_xlsx_Que_127 = "''",
      password_Checks_xlsx_Que_127 = "''",
      jdbcUrl_Checks_xlsx_Que_151 = "''",
      username_Checks_xlsx_Que_151 = "''",
      password_Checks_xlsx_Que_151 = "''",
      jdbcUrl_Trades_xlsx_Que_189 = "''",
      username_Trades_xlsx_Que_189 = "''",
      password_Trades_xlsx_Que_189 = "''",
      jdbcUrl_Checks_xlsx_Que_149 = "''",
      username_Checks_xlsx_Que_149 = "''",
      password_Checks_xlsx_Que_149 = "''",
      jdbcUrl_Checks_xlsx_Que_70 = "''",
      username_Checks_xlsx_Que_70 = "''",
      password_Checks_xlsx_Que_70 = "''",
      jdbcUrl_FilteredRisk_xl_187 = "''",
      username_FilteredRisk_xl_187 = "''",
      password_FilteredRisk_xl_187 = "''",
      jdbcUrl_Result_xlsx_Que_29 = "''",
      username_Result_xlsx_Que_29 = "''",
      password_Result_xlsx_Que_29 = "''",
      workflow_name = "'FVH_IS_AMER_Repo_Significance_Testing_v1_13_1_'"
    )
)

with Pipeline(args) as pipeline:
    checks_xlsx_que_127 = Process(
        name = "Checks_xlsx_Que_127",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          )
        )
    )
    checks_xlsx_que_141 = Process(
        name = "Checks_xlsx_Que_141",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          )
        )
    )
    checks_xlsx_que_149 = Process(
        name = "Checks_xlsx_Que_149",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          )
        )
    )
    checks_xlsx_que_151 = Process(
        name = "Checks_xlsx_Que_151",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          )
        )
    )
    checks_xlsx_que_44 = Process(
        name = "Checks_xlsx_Que_44",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          )
        )
    )
    checks_xlsx_que_62 = Process(
        name = "Checks_xlsx_Que_62",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          )
        )
    )
    checks_xlsx_que_70 = Process(
        name = "Checks_xlsx_Que_70",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Checks.xlsx"
          )
        )
    )
    dynamicinput_207 = Process(
        name = "DynamicInput_207",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
    dynamicinput_218 = Process(
        name = "DynamicInput_218",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
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
    fvh_is_amer_repo_significance_testing_v1_13_1___alteryxselect_42 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AlteryxSelect_42",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AlteryxSelect_42")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_223 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_223",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_223"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_227 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_227",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_227"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_228 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_228",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_228"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_230 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_230",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_230"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_232 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_232",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_232"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_234 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_234",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_234"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_237 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_237",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_237"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_239 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_239",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_239"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_241 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_241",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_241"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_243 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_243",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_243"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_245 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_245",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_245"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_247 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_247",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_247"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_249 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_249",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_249"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_251 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_251",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_251"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_253 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_253",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_253"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_255 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_255",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_255"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_256 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_256",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AppendFields_256"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_207_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_207_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_207_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_208_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_208_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_208_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_209_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_209_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_209_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_211_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_211_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_211_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_212_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_212_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_212_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_213_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_213_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_213_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_214_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_214_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_214_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_215_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_215_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_215_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_218_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_218_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_218_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_219_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_219_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_219_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_220_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_220_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_220_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_221_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_221_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_221_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_262_readlistpathsheet_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_262_readListPathSheet_0",
        properties = ModelTransform(
          modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___DynamicInput_262_readListPathSheet_0"
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___filter_264 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Filter_264",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Filter_264")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___filter_264_reject = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Filter_264_reject",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Filter_264_reject")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___formula_170_to_formula_59_2 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_170_to_Formula_59_2",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_170_to_Formula_59_2")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___formula_204_1 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_204_1",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_204_1")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___formula_235_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_235_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_235_0")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___formula_31_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_31_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_31_0"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___formula_56_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_56_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_56_0")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___formula_57_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_57_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_57_0"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___formula_8_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_8_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_8_0"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___formula_90_1 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_90_1",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_90_1"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___join_140_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_140_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_140_inner"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___join_142_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_142_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_142_inner"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___join_150_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_150_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_150_inner"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___join_168_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_168_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_168_inner"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___join_17_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_17_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_17_inner"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___join_190_inner = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_190_inner",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_190_inner"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___join_190_left = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_190_left",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_190_left"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___join_72_inner_formula_0 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_72_inner_formula_0",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_72_inner_formula_0")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___summarize_139 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Summarize_139",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Summarize_139")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___summarize_175 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Summarize_175",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Summarize_175")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___summarize_60 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Summarize_60",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Summarize_60")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___textinput_51_cast = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___TextInput_51_cast",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___TextInput_51_cast")
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___union_191_postrename = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_191_postRename",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_191_postRename"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___union_197_postrename = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_197_postRename",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_197_postRename"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___union_36_postrename = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_36_postRename",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_36_postRename"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___union_41_postrename = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_41_postRename",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_41_postRename"),
        input_ports = ["in_0", "in_1"]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___unique_65 = Process(
        name = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Unique_65",
        properties = ModelTransform(modelName = "FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Unique_65"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    filteredrisk_xl_15 = Process(
        name = "FilteredRisk_xl_15",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\FilteredRisk.xlsx"
          )
        )
    )
    filteredrisk_xl_187 = Process(
        name = "FilteredRisk_xl_187",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\FilteredRisk.xlsx"
          )
        )
    )
    filteredrisk_xl_37 = Process(
        name = "FilteredRisk_xl_37",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\FilteredRisk.xlsx"
          )
        )
    )
    mtminreporting__164 = Process(
        name = "MTMinreporting__164",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\MTM in reporting.xlsx"
          )
        )
    )
    missingthreshol_53 = Process(
        name = "MissingThreshol_53",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.CsvWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Missing Threshold.csv"
          )
        )
    )
    reporting_xlsx__93 = Process(
        name = "Reporting_xlsx__93",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Reporting.xlsx"
          )
        )
    )
    result_xlsx_que_29 = Process(
        name = "Result_xlsx_Que_29",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Result.xlsx"
          )
        )
    )
    sigtestoutput_x_174 = Process(
        name = "Sigtestoutput_x_174",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Sig test output.xlsx"
          )
        )
    )
    textinput_166 = Process(
        name = "TextInput_166",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_FVH_IS_AMER_Repo_Significance_Testing_v1_13_1__166", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_203 = Process(
        name = "TextInput_203",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_FVH_IS_AMER_Repo_Significance_Testing_v1_13_1__203", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_51 = Process(
        name = "TextInput_51",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_FVH_IS_AMER_Repo_Significance_Testing_v1_13_1__51", sourceType = "Seed")
        ),
        input_ports = None
    )
    trades_xlsx_que_189 = Process(
        name = "Trades_xlsx_Que_189",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\Trades.xlsx"
          )
        )
    )
    mtm_xlsx_query__163 = Process(
        name = "mtm_xlsx_Query__163",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\Alteryx\\Fixed Income Financing\\FVH_IS_Repo_Significance_Testing\\Output\\June Test\\mtm.xlsx"
          )
        )
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_237 >> filteredrisk_xl_187
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_227 >> filteredrisk_xl_37
    textinput_166 >> fvh_is_amer_repo_significance_testing_v1_13_1___join_168_inner._in(1)
    dynamicinput_218 >> fvh_is_amer_repo_significance_testing_v1_13_1___unique_65._in(1)
    dynamicinput_208 >> fvh_is_amer_repo_significance_testing_v1_13_1___unique_65._in(0)
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_262_readlistpathsheet_0 >> dynamicinput_262
    dynamicinput_221 >> fvh_is_amer_repo_significance_testing_v1_13_1___join_168_inner._in(0)
    textinput_51 >> fvh_is_amer_repo_significance_testing_v1_13_1___textinput_51_cast
    dynamicinput_219 >> fvh_is_amer_repo_significance_testing_v1_13_1___unique_65._in(2)
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___formula_31_0._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___union_36_postrename._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_223._in(0)]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_239 >> trades_xlsx_que_189
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_213_readlistpathsheet_0 >> dynamicinput_213
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_218_readlistpathsheet_0 >> dynamicinput_218
    dynamicinput_213 >> fvh_is_amer_repo_significance_testing_v1_13_1___formula_56_0
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___formula_235_0._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_228._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_237._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_239._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_241._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_243._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_230._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_232._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_247._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_255._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_249._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_253._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_251._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_256._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_245._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_227._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_223._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_234._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_262_readlistpathsheet_0._in(0)]
    )
    (
        dynamicinput_262._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___union_197_postrename._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___union_197_postrename._in(3)]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_245 >> sigtestoutput_x_174
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___join_72_inner_formula_0._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___summarize_139._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_243._in(0)]
    )
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___unique_65._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_247._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___union_197_postrename._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_234._in(0)]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_221_readlistpathsheet_0 >> dynamicinput_221
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_232 >> missingthreshol_53
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___alteryxselect_42._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___union_36_postrename._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___union_36_postrename._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___union_41_postrename._in(1)]
    )
    (
        dynamicinput_214._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_237._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___union_191_postrename._in(1)]
    )
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___union_191_postrename._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_239._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_190_left._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_190_inner._in(1)]
    )
    dynamicinput_220 >> fvh_is_amer_repo_significance_testing_v1_13_1___union_191_postrename._in(2)
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_234 >> mtm_xlsx_query__163
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_215_readlistpathsheet_0 >> dynamicinput_215
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_247 >> checks_xlsx_que_62
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_253 >> reporting_xlsx__93
    dynamicinput_211 >> fvh_is_amer_repo_significance_testing_v1_13_1___alteryxselect_42
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___formula_204_1._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___formula_8_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___formula_8_0._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___unique_65._in(3),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_207_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_211_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_214_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_220_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_209_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_215_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_219_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_221_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_218_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_212_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___formula_235_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_213_readlistpathsheet_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_208_readlistpathsheet_0._in(0)]
    )
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___join_190_left._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___join_72_inner_formula_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_230._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_256._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_150_inner._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___formula_57_0._in(0)]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_223 >> filteredrisk_xl_15
    (
        dynamicinput_207._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___formula_31_0._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_17_inner._in(0)]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_251 >> mtminreporting__164
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___textinput_51_cast._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___formula_57_0._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___union_191_postrename._in(4)]
    )
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___summarize_60._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_247._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___union_197_postrename._in(0)]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_241 >> checks_xlsx_que_141
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_211_readlistpathsheet_0 >> dynamicinput_211
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___formula_8_0._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___formula_31_0._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_17_inner._in(1)]
    )
    dynamicinput_209 >> fvh_is_amer_repo_significance_testing_v1_13_1___union_191_postrename._in(3)
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___formula_90_1._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___join_142_inner._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_251._in(1)]
    )
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___formula_56_0._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_232._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_168_inner._in(2)]
    )
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___join_142_inner._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_255._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_253._in(0)]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_214_readlistpathsheet_0 >> dynamicinput_214
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_207_readlistpathsheet_0 >> dynamicinput_207
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___formula_57_0._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_232._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_168_inner._in(3)]
    )
    dynamicinput_212 >> fvh_is_amer_repo_significance_testing_v1_13_1___union_41_postrename._in(0)
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___join_168_inner._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___filter_264._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___filter_264_reject._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___formula_170_to_formula_59_2._in(0)]
    )
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___union_41_postrename._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_230._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___formula_57_0._in(1)]
    )
    textinput_203 >> fvh_is_amer_repo_significance_testing_v1_13_1___formula_204_1
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___summarize_175._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_237._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___union_191_postrename._in(0)]
    )
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___summarize_139._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_241._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_140_inner._in(0)]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_212_readlistpathsheet_0 >> dynamicinput_212
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_230 >> checks_xlsx_que_44
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_220_readlistpathsheet_0 >> dynamicinput_220
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_249 >> result_xlsx_que_29
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___formula_170_to_formula_59_2._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___join_142_inner._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___formula_90_1._in(2),
              fvh_is_amer_repo_significance_testing_v1_13_1___summarize_60._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_245._in(1)]
    )
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___union_36_postrename._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_228._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_190_left._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_190_inner._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___summarize_175._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_227._in(0)]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_228 >> checks_xlsx_que_127
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_256 >> checks_xlsx_que_151
    (
        dynamicinput_215._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_241._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_140_inner._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___formula_90_1._in(1)]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_209_readlistpathsheet_0 >> dynamicinput_209
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_219_readlistpathsheet_0 >> dynamicinput_219
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_243 >> checks_xlsx_que_70
    (
        fvh_is_amer_repo_significance_testing_v1_13_1___union_197_postrename._out(0)
        >> [fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_255._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_249._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_142_inner._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___formula_90_1._in(0),
              fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_256._in(1),
              fvh_is_amer_repo_significance_testing_v1_13_1___join_150_inner._in(1)]
    )
    fvh_is_amer_repo_significance_testing_v1_13_1___appendfields_255 >> checks_xlsx_que_149
    fvh_is_amer_repo_significance_testing_v1_13_1___dynamicinput_208_readlistpathsheet_0 >> dynamicinput_208
