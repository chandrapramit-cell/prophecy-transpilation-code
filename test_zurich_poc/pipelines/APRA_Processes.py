from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "APRA_Processes",
    version = 1,
    auto_layout = False,
    params = Parameters(
      Question__Text_Box_392 = "''",
      jdbcUrl_NoAPRAUnmapped__598 = "''",
      Question__Check_Box_382 = "''",
      Question__Check_Box_381 = "''",
      workflow_name = "'APRA_Processes'",
      Question__Check_Box_384 = "''",
      Question__Check_Box_399 = "''",
      password_NoAPRAUnmapped__598 = "''",
      Question__File_Browse_386 = "''",
      jdbcUrl_aka_SnowflakePR_445 = "''",
      File_Browse_388 = "'PLACEHOLDER'",
      Question__Text_Box_390 = "''",
      jdbcUrl_PLACEHOLDER_488 = "''",
      password_PLACEHOLDER_488 = "''",
      File_Browse_386 = "'PLACEHOLDER'",
      Question__Text_Box_401 = "''",
      username_NoAPRAUnmapped__598 = "''",
      username_PLACEHOLDER_488 = "''",
      password_aka_SnowflakePR_445 = "''",
      Question__File_Browse_388 = "''",
      username_aka_SnowflakePR_445 = "''"
    )
)

with Pipeline(args) as pipeline:
    textinput_98 = Process(
        name = "TextInput_98",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_98", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_193 = Process(
        name = "TextInput_193",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_193", sourceType = "Seed")
        ),
        input_ports = None
    )
    apra_processes__formula_579_0 = Process(
        name = "APRA_Processes__Formula_579_0",
        properties = ModelTransform(modelName = "APRA_Processes__Formula_579_0")
    )
    apra_processes__filter_580 = Process(
        name = "APRA_Processes__Filter_580",
        properties = ModelTransform(modelName = "APRA_Processes__Filter_580")
    )
    apra_processes__noapraunmapped__598 = Process(
        name = "APRA_Processes__NoAPRAUnmapped__598",
        properties = ModelTransform(modelName = "APRA_Processes__NoAPRAUnmapped__598")
    )
    directory_604 = Process(
        name = "Directory_604",
        properties = Directory(
          path = "\\\\ms.zurich.com.au\\DFS\\Wan\\CommonData\\MainframeReports\\",
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
    mappings_xlsx_q_578 = Process(
        name = "Mappings_xlsx_Q_578",
        properties = SFTPSource(
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
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
          properties = SFTPSource.SFTPSourceInternal(
            filePath = "\\\\ms.zurich.com.au\\DFS\\AlteryX\\Prod\\Finance\\Reinsurance Processes\\MHC and APRA Reporting\\02_Mapping\\Mappings.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/APRA_Processes/Mappings_xlsx_Q_578.yml")
        ),
        input_ports = None
    )
    apraunmapped_xl_492 = Process(
        name = "APRAUnmapped_xl_492",
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
          properties = SFTPTarget.SFTPTargetInternal(filePath = "APRA Unmapped.xlsx")
        )
    )
    apra_processes__formula_618_1 = Process(
        name = "APRA_Processes__Formula_618_1",
        properties = ModelTransform(modelName = "APRA_Processes__Formula_618_1")
    )
    dynamicinput_617 = Process(
        name = "DynamicInput_617",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          tableIntegration = "oracle",
          tableConnector = "transpiled_connection",
          fileType = "fileType_XLSX",
          sqlQuery = "\\\\ms.zurich.com.au\\dfs\\AlteryX\\Dev\\Finance\\TM1_Data_Migration\\Cube\\Inputs\\Archive\\S2TP_01112024.025254.TXT",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection"
        ),
        is_custom_output_schema = True
    )
    apra_processes__formula_249_to_formula_258_2 = Process(
        name = "APRA_Processes__Formula_249_to_Formula_258_2",
        properties = ModelTransform(modelName = "APRA_Processes__Formula_249_to_Formula_258_2")
    )
    textinput_635 = Process(
        name = "TextInput_635",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_635", sourceType = "Seed")
        ),
        input_ports = None
    )
    apra_processes__email_514_attach = Process(
        name = "APRA_Processes__Email_514_attach",
        properties = ModelTransform(modelName = "APRA_Processes__Email_514_attach")
    )
    dynamicinput_192 = Process(
        name = "DynamicInput_192",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [{"textToReplace" : "pPeriodEnd_INPUT", "textToReplaceField" : "pPeriodEnd"}],
          tableIntegration = "oracle",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = ["pPeriod"],
          fileType = "fileType_XLSX",
          sqlQuery = "DECLARE @pPeriodEnd VARCHAR(16);\nSET @pPeriodEnd = 'pPeriodEnd_INPUT';\n\nSELECT \n\t  SUBSTRING([EXT_ACCV_ACCT_KEY_01],2,1) AS v_Company\n      ,SUBSTRING([EXT_ACCV_ACCT_KEY_01],3,7) AS v_Reinsurer\n      ,[EXT_ACCV_ACCT_BRCH_01] AS v_RIBranch\n\t  ,[EXT_ACCV_TZAC_CODE] AS v_TZAC\n      ,[EXT_ACCV_ACT_TYPE] AS v_AccountType\n\t  ,POL_STMT.EXT_STMT_NETT_AMT AS v_OSDebtors\n\t  ,CONCAT (POL_STMT.EXT_STMT_DESC1, ' ', POL_STMT.EXT_STMT_DESC2) AS v_DebtorsDesc\n\t  ,POL_STMT.EXT_STMT_DATE_EFFECT AS v_DateEffect\n\t  ,SUBSTRING(POL_STMT.EXT_STMT_BATC_KEY, 10,4) AS v_TransType\n\t  ,CASE WHEN SUBSTRING(POL_STMT.EXT_STMT_BATC_KEY, 10,4)='Q025'\n                   OR SUBSTRING(POL_STMT.EXT_STMT_BATC_KEY, 10,4)='C004'\n                   OR SUBSTRING(POL_STMT.EXT_STMT_BATC_KEY, 10,4)='C013'\n                   OR POL_STMT.EXT_STMT_JOURNAL_FLAG = 'C'\n\t\t\tTHEN 'C'\n\t\t\tELSE 'P'\n\t\t\tEND AS v_Type\n\t  ,POL_STMT.CTL_SRC_ROW_ID AS v_STMTID\n\t  ,@pPeriodEnd AS v_EndOfMonth\n\t  ,CAST(CAST([EXT_STMT_DUE_YEAR] AS VARCHAR(4)) + \n\t\tRIGHT('0' + CAST([EXT_STMT_DUE_MONTH] AS VARCHAR(2)),2) +\n\t\tSUBSTRING(CAST([EXT_STMT_DATE_EFFECT] AS VARCHAR(8)),7,2)\n\t\tAS decimal) AS v_DateCalc\n  FROM [FINDB].[src].[POL_ACCV_01]\n  LEFT JOIN [FINDB].[src].[POL_STMT] ON POL_ACCV_01.EXT_ACCV_ACCT_KEY_01 = POL_STMT.EXT_STMT_ACNO_KEY\n  WHERE EXT_ACCV_CODE = '01'\n  AND EXT_ACCV_VALID_FLAG = 1\n  AND EXT_ACCV_ACT_TYPE IN('OL', 'OP', 'LP', 'TP', 'TX')\n  AND POL_STMT.EXT_STMT_MARRY_FLAG IS NULL\n  AND POL_STMT.EXT_STMT_NETT_AMT IS NOT NULL\n  ORDER BY v_STMTID",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection"
        ),
        is_custom_output_schema = True
    )
    apra_processes__unique_495 = Process(
        name = "APRA_Processes__Unique_495",
        properties = ModelTransform(modelName = "APRA_Processes__Unique_495"),
        input_ports = ["in_0", "in_1"]
    )
    apra_processes__summarize_426 = Process(
        name = "APRA_Processes__Summarize_426",
        properties = ModelTransform(modelName = "APRA_Processes__Summarize_426")
    )
    textinput_5 = Process(
        name = "TextInput_5",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_5", sourceType = "Seed")
        ),
        input_ports = None
    )
    apra_processes__formula_446_0 = Process(
        name = "APRA_Processes__Formula_446_0",
        properties = ModelTransform(modelName = "APRA_Processes__Formula_446_0"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14", "in_15", "in_16", "in_17", "in_18", "in_19", "in_20", "in_21", "in_22", "in_23",
         "in_24", "in_25", "in_26", "in_27", "in_28", "in_29", "in_30", "in_31", "in_32", "in_33", "in_34",
         "in_35", "in_36", "in_37", "in_38", "in_39", "in_40", "in_41", "in_42", "in_43", "in_44", "in_45",
         "in_46", "in_47", "in_48", "in_49", "in_50"]
    )
    apra_processes__filter_631 = Process(
        name = "APRA_Processes__Filter_631",
        properties = ModelTransform(modelName = "APRA_Processes__Filter_631"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    apra_processes__join_494_inner = Process(
        name = "APRA_Processes__Join_494_inner",
        properties = ModelTransform(modelName = "APRA_Processes__Join_494_inner"),
        input_ports = ["in_0", "in_1"]
    )
    apra_processes__aka_snowflakepr_445 = Process(
        name = "APRA_Processes__aka_SnowflakePR_445",
        properties = ModelTransform(modelName = "APRA_Processes__aka_SnowflakePR_445")
    )
    apra_processes__join_477_inner = Process(
        name = "APRA_Processes__Join_477_inner",
        properties = ModelTransform(modelName = "APRA_Processes__Join_477_inner"),
        input_ports = ["in_0", "in_1"]
    )
    textinput_337 = Process(
        name = "TextInput_337",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_337", sourceType = "Seed")
        ),
        input_ports = None
    )
    dynamicinput_39 = Process(
        name = "DynamicInput_39",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          tableIntegration = "oracle",
          tableConnector = "transpiled_connection",
          fileType = "fileType_XLSX",
          sqlQuery = "`TZAC$`",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection"
        ),
        is_custom_output_schema = True
    )
    apra_processes__filter_582 = Process(
        name = "APRA_Processes__Filter_582",
        properties = ModelTransform(modelName = "APRA_Processes__Filter_582")
    )
    apra_processes__formula_99_to_formula_183_2 = Process(
        name = "APRA_Processes__Formula_99_to_Formula_183_2",
        properties = ModelTransform(modelName = "APRA_Processes__Formula_99_to_Formula_183_2")
    )
    apra_processes__formula_630_0 = Process(
        name = "APRA_Processes__Formula_630_0",
        properties = ModelTransform(modelName = "APRA_Processes__Formula_630_0")
    )
    apra_processes__formula_532_0 = Process(
        name = "APRA_Processes__Formula_532_0",
        properties = ModelTransform(modelName = "APRA_Processes__Formula_532_0")
    )
    apra_processes__placeholder_488 = Process(
        name = "APRA_Processes__PLACEHOLDER_488",
        properties = ModelTransform(modelName = "APRA_Processes__PLACEHOLDER_488")
    )
    apra_processes__appendfields_641 = Process(
        name = "APRA_Processes__AppendFields_641",
        properties = ModelTransform(modelName = "APRA_Processes__AppendFields_641"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7"]
    )
    email_514 = Process(
        name = "Email_514",
        properties = Email(
          body = "Table",
          bodyColumn = "Table",
          subject = "APRA Output ran successfully",
          subjectFromColumn = False,
          showCcBcc = True,
          to = "To_Email_Address",
          toFromColumn = True,
          bodyFromColumn = True,
          toColumn = "To_Email_Address",
          ccFromColumn = True,
          bccFromColumn = False,
          ccColumn = "Cc_Email_Address",
          cc = "Cc_Email_Address",
          subjectColumn = "APRA Output ran successfully",
          connection = "transpiled_connection"
        ),
        input_ports = 2
    )
    apra_processes__union_482 = Process(
        name = "APRA_Processes__Union_482",
        properties = ModelTransform(modelName = "APRA_Processes__Union_482"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    apra_processes__unique_489 = Process(
        name = "APRA_Processes__Unique_489",
        properties = ModelTransform(modelName = "APRA_Processes__Unique_489"),
        input_ports = ["in_0", "in_1"]
    )
    dynamicinput_427 = Process(
        name = "DynamicInput_427",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [{"textToReplace" : "ytdPeriod_INPUT", "textToReplaceField" : "ytdPeriod"}],
          tableIntegration = "oracle",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = ["pPeriod"],
          fileType = "fileType_XLSX",
          sqlQuery = "SELECT\n       SUBSTRING([EXT_RTRN_BATC_KEY],4,4) AS v_AccountingYear,\n       SUBSTRING([EXT_RTRN_BATC_KEY],8,2) AS v_AccountingMonth,\n       SUBSTRING([EXT_RTRN_RI_ACCT],2,1) AS v_Company,\n       POL_ACCV_01.EXT_ACCV_ACCT_BRCH_01 AS v_AccountBranch,\n       SUBSTRING([EXT_RTRN_RI_ACCT],3,7) AS v_Reinsurer,\n       POL_ACCV_01.EXT_ACCV_ACT_NAME AS v_RIName,\n       POL_ACCV_01.EXT_ACCV_TZAC_CODE AS v_TZAC,\n       POL_ACCV_01.EXT_ACCV_ACT_TYPE AS v_AccountType,\n       SUM([EXT_RTRN_RI_PREM]) AS v_RWP,\n       POL_POLY.EXT_POLY_POL_BRANCH AS v_PolBranch,\n       SUBSTRING([EXT_RTRN_POLY_KEY],2,7) AS v_Policy,\n       SUBSTRING([EXT_RTRN_POLY_KEY],9,3) AS v_ProdClass,\n       [EXT_RTRN_RISK_NUMBER] AS v_RiskNo,\n       [EXT_RTRN_RISK_CLASS] AS v_RiskClass,\n       [EXT_RTRN_PREM_CLASS] AS v_PremClass\n  FROM [FINDB].[src].[POL_RTRN]\n  LEFT JOIN [FINDB].[src].[POL_ACCV_01] ON POL_RTRN.EXT_RTRN_RI_ACCT = POL_ACCV_01.EXT_ACCV_ACCT_KEY_01\n  LEFT JOIN [FINDB].[src].[POL_POLY] ON POL_RTRN.EXT_RTRN_POLY_KEY = CONCAT(POL_POLY.EXT_POLY_POL_COMPANY, POL_POLY.EXT_POLY_POLICY, POL_POLY.EXT_POLY_POLS_TYPE)\n  WHERE \n  SUBSTRING([EXT_RTRN_BATC_KEY],4,6) in (ytdPeriod_INPUT)\n  GROUP BY\n       SUBSTRING([EXT_RTRN_BATC_KEY],4,4),\n       SUBSTRING([EXT_RTRN_BATC_KEY],8,2),\n       SUBSTRING([EXT_RTRN_RI_ACCT],2,1),\n       POL_ACCV_01.EXT_ACCV_ACCT_BRCH_01,\n       SUBSTRING([EXT_RTRN_RI_ACCT],3,7),\n       POL_ACCV_01.EXT_ACCV_ACT_NAME,\n       POL_ACCV_01.EXT_ACCV_TZAC_CODE,\n       POL_ACCV_01.EXT_ACCV_ACT_TYPE,\n       POL_POLY.EXT_POLY_POL_BRANCH,\n       SUBSTRING([EXT_RTRN_POLY_KEY],2,7),\n       SUBSTRING([EXT_RTRN_POLY_KEY],9,3),\n       [EXT_RTRN_RISK_NUMBER],\n       [EXT_RTRN_RISK_CLASS],\n       [EXT_RTRN_PREM_CLASS]",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_464 = Process(
        name = "DynamicInput_464",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          tableIntegration = "oracle",
          tableConnector = "transpiled_connection",
          fileType = "fileType_XLSX",
          sqlQuery = "`RI_ACCOUNT$`",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_97 = Process(
        name = "DynamicInput_97",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [{"textToReplace" : "pPeriod_INPUT", "textToReplaceField" : "pPeriod"}],
          tableIntegration = "oracle",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = ["pPeriod"],
          fileType = "fileType_XLSX",
          sqlQuery = "DECLARE @pPeriod VARCHAR(6);\nSET @pPeriod = 'pPeriod_INPUT';\n\nDECLARE @sYearStart VARCHAR(10);\nSET @sYearStart = SUBSTRING(@pPeriod, 1, 4) + '0101';\n\nSELECT\n      SUBSTRING([EXT_CLRI_ACCT_KEY_CR],2,1) AS v_Company\n      ,SUBSTRING([EXT_CLRI_ACCT_KEY_CR],3,7) AS v_Reinsurer\n      ,[EXT_CLRI_PERIOD_CR] AS v_Period0\n      ,[EXT_CLRI_PERIOD_CR1] AS v_Period1\n      ,[EXT_CLRI_PERIOD_CR2] AS v_Period2\n      ,[EXT_CLRI_PERIOD_CR3] AS v_Period3\n      ,[EXT_CLRI_BAL_OUT_CR] AS v_BalOut0\n      ,[EXT_CLRI_BAL_OUT_CR1] AS v_BalOut1\n      ,[EXT_CLRI_BAL_OUT_CR2] AS v_BalOut2\n      ,[EXT_CLRI_BAL_OUT_CR3] AS v_BalOut3\n\u2002\u2002\u2002\u2002\u2002\u2002  ,POL_ACCV_01.EXT_ACCV_ACT_TYPE AS v_AccountType\n\u2002\u2002\u2002\u2002\u2002\u2002  ,POL_ACCV_01.EXT_ACCV_ACCT_BRCH_01 AS v_AccountBranch\n\u2002\u2002\u2002\u2002\u2002\u2002  ,POL_ACCV_01.EXT_ACCV_ACT_NAME AS v_AccountName\n\u2002\u2002\u2002\u2002\u2002\u2002  ,POL_ACCV_01.EXT_ACCV_TZAC_CODE AS v_TZAC\n\u2002\u2002\u2002\u2002\u2002\u2002  ,LEFT(POL_CLAV_CA.[EXT_CLAV_DATE_OCC_CA],4) AS v_AccidentYear\n\u2002\u2002\u2002\u2002\u2002\u2002  ,POL_POLY.EXT_POLY_POL_BRANCH AS v_PolBranch\n\u2002\u2002\u2002\u2002\u2002\u2002  ,SUBSTRING(POL_CLAV_CA.EXT_CLAV_RISK_KEY_CA,2,7) AS v_Policy\n\u2002\u2002\u2002\u2002\u2002\u2002  ,SUBSTRING(POL_CLAV_CA.EXT_CLAV_RISK_KEY_CA,9,3) AS v_ProdClass\n\u2002\u2002\u2002\u2002\u2002\u2002  ,SUBSTRING(POL_CLAV_CA.EXT_CLAV_RISK_KEY_CA,21,4) AS v_RiskNo\n\u2002\u2002\u2002\u2002\u2002\u2002  ,POL_CLAV_CA.EXT_CLAV_RSK_CLASS_CA AS v_RiskClass\n\u2002\u2002\u2002\u2002\u2002\u2002  ,POL_CLAV_CA.EXT_CLAV_SUB_CLASS_CA AS v_PremClass\n\u2002\u2002\u2002\u2002\u2002\u2002  ,POL_CLAV_CA.EXT_CLAV_CLAM_BRCH_CA AS v_ClaimBranch\n\u2002\u2002\u2002\u2002\u2002\u2002  ,SUBSTRING(POL_CLAV_CA.EXT_CLAV_CLAM_KEY_CA,3,7) AS v_ClaimNumber\n\u2002\u2002\u2002\u2002\u2002\u2002  ,[EXT_CLRI_PPN_CR] AS v_Proportion\n  FROM [FINDB].[src].[POL_CLRI]\n  LEFT JOIN [FINDB].[src].[POL_CLAV_CA] ON POL_CLRI.EXT_CLRI_CLAM_KEY_CR = POL_CLAV_CA.EXT_CLAV_CLAM_KEY_CA\n  LEFT JOIN [FINDB].[src].[POL_ACCV_01] ON POL_CLRI.EXT_CLRI_ACCT_KEY_CR = POL_ACCV_01.EXT_ACCV_ACCT_KEY_01\n  LEFT JOIN [FINDB].[src].[POL_POLY] ON SUBSTRING(POL_CLAV_CA.EXT_CLAV_RISK_KEY_CA,1,11) = CONCAT(POL_POLY.EXT_POLY_POL_COMPANY, POL_POLY.EXT_POLY_POLICY, POL_POLY.EXT_POLY_POLS_TYPE)\n  WHERE EXT_CLRI_TYPE_CR IN('RI','EL','CF')\n  AND (POL_CLAV_CA.EXT_CLAV_CLAIM_STATUS_CA <> 'C'\n  OR(POL_CLAV_CA.EXT_CLAV_CLAIM_STATUS_CA = 'C' AND POL_CLAV_CA.EXT_CLAV_STATUS_DATE_CA >= CAST(@sYearStart as int)))",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection"
        ),
        is_custom_output_schema = True
    )
    apraunmapped_xl_478 = Process(
        name = "APRAUnmapped_xl_478",
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
          properties = SFTPTarget.SFTPTargetInternal(filePath = "APRA Unmapped.xlsx")
        )
    )
    dynamicinput_614 = Process(
        name = "DynamicInput_614",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          tableIntegration = "oracle",
          tableConnector = "transpiled_connection",
          fileType = "fileType_XLSX",
          sqlQuery = "\\\\ms.zurich.com.au\\dfs\\AlteryX\\Dev\\Finance\\TM1_Data_Migration\\Cube\\Inputs\\Archive\\S2TP_01112024.025254.TXT",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection"
        ),
        is_custom_output_schema = True
    )
    emailrecipients_518 = Process(
        name = "Emailrecipients_518",
        properties = SFTPSource(
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
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
          properties = SFTPSource.SFTPSourceInternal(
            filePath = "\\\\ms.zurich.com.au\\DFS\\AlteryX\\Prod\\Finance\\Reinsurance Processes\\MHC and APRA Reporting\\02_Mapping\\Email recipients.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/APRA_Processes/Emailrecipients_518.yml")
        ),
        input_ports = None
    )
    apra_processes__formula_538_0 = Process(
        name = "APRA_Processes__Formula_538_0",
        properties = ModelTransform(modelName = "APRA_Processes__Formula_538_0")
    )
    apra_processes__formula_8_to_formula_23_1 = Process(
        name = "APRA_Processes__Formula_8_to_Formula_23_1",
        properties = ModelTransform(modelName = "APRA_Processes__Formula_8_to_Formula_23_1")
    )
    apra_processes__formula_539_to_formula_246_1 = Process(
        name = "APRA_Processes__Formula_539_to_Formula_246_1",
        properties = ModelTransform(modelName = "APRA_Processes__Formula_539_to_Formula_246_1")
    )
    apra_processes__formula_615_1 = Process(
        name = "APRA_Processes__Formula_615_1",
        properties = ModelTransform(modelName = "APRA_Processes__Formula_615_1")
    )
    apra_processes__filter_580 >> dynamicinput_464
    directory_604 >> apra_processes__formula_630_0
    apra_processes__formula_615_1 >> dynamicinput_614
    apra_processes__formula_618_1 >> dynamicinput_617
    (
        apra_processes__formula_630_0._out(0)
        >> [apra_processes__formula_615_1._in(0), apra_processes__formula_618_1._in(0)]
    )
    textinput_98 >> apra_processes__formula_538_0
    (
        apra_processes__formula_99_to_formula_183_2._out(0)
        >> [apra_processes__formula_446_0._in(43), apra_processes__formula_446_0._in(44),
              apra_processes__union_482._in(1)]
    )
    apra_processes__email_514_attach >> email_514._in(1)
    textinput_193 >> apra_processes__formula_539_to_formula_246_1
    dynamicinput_614 >> apra_processes__filter_631._in(0)
    (
        apra_processes__union_482._out(0)
        >> [apra_processes__noapraunmapped__598._in(0), apra_processes__unique_489._in(1),
              apra_processes__join_477_inner._in(1), apra_processes__unique_495._in(1),
              apra_processes__join_494_inner._in(1)]
    )
    textinput_635 >> apra_processes__appendfields_641._in(5)
    dynamicinput_427 >> apra_processes__formula_8_to_formula_23_1
    textinput_337 >> apra_processes__filter_631._in(2)
    (
        apra_processes__formula_249_to_formula_258_2._out(0)
        >> [apra_processes__formula_446_0._in(45), apra_processes__union_482._in(2)]
    )
    apra_processes__filter_582 >> dynamicinput_39
    apra_processes__appendfields_641._out(0) >> [email_514._in(0), apra_processes__email_514_attach._in(0)]
    apra_processes__formula_538_0 >> dynamicinput_97
    (
        dynamicinput_464._out(0)
        >> [apra_processes__formula_446_0._in(0), apra_processes__formula_446_0._in(2),
              apra_processes__formula_446_0._in(4), apra_processes__formula_446_0._in(6),
              apra_processes__formula_446_0._in(8), apra_processes__formula_446_0._in(10),
              apra_processes__formula_446_0._in(12), apra_processes__formula_446_0._in(14),
              apra_processes__formula_446_0._in(18), apra_processes__formula_446_0._in(26),
              apra_processes__formula_446_0._in(29), apra_processes__formula_446_0._in(30),
              apra_processes__formula_446_0._in(32), apra_processes__formula_446_0._in(38),
              apra_processes__formula_446_0._in(39), apra_processes__unique_489._in(0),
              apra_processes__join_477_inner._in(0)]
    )
    (
        apra_processes__formula_532_0._out(0)
        >> [apra_processes__appendfields_641._in(7), apra_processes__placeholder_488._in(0)]
    )
    dynamicinput_192 >> apra_processes__formula_249_to_formula_258_2
    dynamicinput_617 >> apra_processes__filter_631._in(1)
    (
        dynamicinput_39._out(0)
        >> [apra_processes__formula_446_0._in(16), apra_processes__formula_446_0._in(19),
              apra_processes__formula_446_0._in(21), apra_processes__formula_446_0._in(22),
              apra_processes__formula_446_0._in(24), apra_processes__formula_446_0._in(27),
              apra_processes__formula_446_0._in(34), apra_processes__formula_446_0._in(36),
              apra_processes__unique_495._in(0), apra_processes__join_494_inner._in(0)]
    )
    apra_processes__filter_631._out(0) >> [apra_processes__formula_446_0._in(31), apra_processes__union_482._in(3)]
    apra_processes__summarize_426 >> dynamicinput_427
    (
        apra_processes__formula_8_to_formula_23_1._out(0)
        >> [apra_processes__formula_446_0._in(41), apra_processes__formula_446_0._in(42),
              apra_processes__union_482._in(0)]
    )
    apra_processes__unique_495._out(0) >> [apraunmapped_xl_492._in(0), apra_processes__appendfields_641._in(4)]
    textinput_5 >> apra_processes__summarize_426
    apra_processes__formula_539_to_formula_246_1 >> dynamicinput_192
    apra_processes__unique_489._out(0) >> [apraunmapped_xl_478._in(0), apra_processes__appendfields_641._in(3)]
    (
        apra_processes__formula_446_0._out(0)
        >> [apra_processes__appendfields_641._in(6), apra_processes__formula_532_0._in(0),
              apra_processes__aka_snowflakepr_445._in(0)]
    )
    dynamicinput_97 >> apra_processes__formula_99_to_formula_183_2
    mappings_xlsx_q_578._out(0) >> [apra_processes__appendfields_641._in(0), apra_processes__formula_579_0._in(0)]
    emailrecipients_518._out(0) >> [apra_processes__appendfields_641._in(1), apra_processes__appendfields_641._in(2)]
    apra_processes__formula_579_0._out(0) >> [apra_processes__filter_580._in(0), apra_processes__filter_582._in(0)]
