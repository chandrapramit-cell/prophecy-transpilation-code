from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "perpetual_inventory",
    version = 1,
    auto_layout = False,
    params = Parameters(
      PASSWORD_ACCOUNTINGTRANS_71 = "''",
      JDBCURL_PERPETUALINVENT_144 = "''",
      PASSWORD_SAMTECFACILITIE_179 = "''",
      QUESTION__TEXT_BOX_186 = "''",
      PASSWORD_ACCOUNTINGTRANS_82 = "''",
      PASSWORD_ACCOUNTINGTRANS_70 = "''",
      WORKFLOW_NAME = "'perpetual_inventory'",
      USERNAME_ACCOUNTINGTRANS_71 = "''",
      QUESTION__LIST_BOX_180 = "''",
      USERNAME_ACCOUNTINGTRANS_70 = "''",
      USERNAME_PERPETUALINVENT_144 = "''",
      PASSWORD_PERPETUALINVENT_144 = "''",
      USERNAME_DSN_R2S_PROD_ED_181 = "''",
      USERNAME_ACCOUNTINGTRANS_82 = "''",
      PASSWORD_DSN_R2S_PROD_ED_181 = "''",
      USERNAME_SAMTECFACILITIE_179 = "''",
      QUESTION__TEXT_BOX_187 = "''"
    )
)

with Pipeline(args) as pipeline:
    perpetual_inventory__alteryxselect_98 = Process(
        name = "perpetual_inventory__AlteryxSelect_98",
        properties = ModelTransform(modelName = "perpetual_inventory__AlteryxSelect_98")
    )
    crosstab_81_rename = Process(
        name = "CrossTab_81_rename",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "import pandas as pd\nactions_in_use = {\"Sum\"}\nactions_without_prefix = {\"First\", \"Last\", \"Sum\"}\n\ndef rename_column(col_name):\n    last_underscore_index = col_name.rfind(\"_\")\n    if last_underscore_index != -1:\n        first_half = col_name[:last_underscore_index]\n        second_half = col_name[last_underscore_index + 1:]\n\n        if second_half == \"Concat\":\n            return first_half\n        elif len(actions_in_use) == 1 and actions_in_use.issubset(actions_without_prefix) and second_half in actions_without_prefix:\n            return first_half\n        elif second_half in actions_in_use:\n            return f\"{second_half}_{first_half}\"\n        else:\n            return col_name\n    else:\n        return col_name\n\nrenamed_columns = [rename_column(col_name) for col_name in in0.columns]\n\nvalid_columns = [\n    (orig_col, renamed_col)\n    for orig_col, renamed_col in zip(in0.columns, renamed_columns)\n    if not renamed_col.endswith(\"_dummy_\")\n]\n\nfiltered_dataframe = in0[[orig_col for orig_col, _ in valid_columns]].copy()\nvalid_renamed_columns = [renamed_col for _, renamed_col in valid_columns]\n\nfiltered_dataframe.columns = valid_renamed_columns\nout0 = filtered_dataframe\n"
        ),
        is_custom_output_schema = True
    )
    dynamicinput_164 = Process(
        name = "DynamicInput_164",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [{"textToReplace" : "1975-12-30", "textToReplaceField" : "PREVIOUSQUARTERCLOSINGDATE"}],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "select ITEMDESC as PartNumber,\n\tFRZCOST as UnitCost,\n\tFRZ5 + PREVFRZ5 as LaborCost,\n\tFRZ3 + PREVFRZ3 as PlatingCost,\n\tPLANNER as PlannerCode,\n\tBUYER as Buyer,\n\tORDPOL as OrderPolicy,\n\tMISCSTAT3 as CustomerSupplied,\n\tSTDCOST as StandardCost,\n\tCOST5 + PREVSTD5 as StandardLaborCost,\n\tCOST3 + PREVSTD3 as StandardPlatingCost \nfrom ODS.Analysis.tvf_bridging__dbo__PCMFG_INV('1975-12-30')",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    perpetual_inventory__alteryxselect_23 = Process(
        name = "perpetual_inventory__AlteryxSelect_23",
        properties = ModelTransform(modelName = "perpetual_inventory__AlteryxSelect_23")
    )
    perpetual_inventory__formula_134_0 = Process(
        name = "perpetual_inventory__Formula_134_0",
        properties = ModelTransform(modelName = "perpetual_inventory__Formula_134_0"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11"]
    )
    dsn_r2s_prod_ed_181 = Process(
        name = "DSN_r2s_prod_ed_181",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "odbc:DSN=r2s-prod-edw-alteryx",
              "username": "${username_DSN_r2s_prod_ed_181}",
              "id": "transpiled_connection",
              "port": "1521",
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = OracleSource.OracleSourceInternal(
            pathSelection = "warehouseQuery",
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "DSN_r2s_prod_ed_181"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select common.part.part_number,\n\tcommon.part.reporting_series,\n\tcommon.part.reporting_series_era,\n\tcommon.part.reporting_series_weave,\n\tcommon.part.reporting_series_block,\n\tcommon.part.reporting_series_segment,\n\tcommon.part.dm_item_type,\n\tcommon.part.pcmfg_item_type \nfrom common.part"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/perpetual_inventory/DSN_r2s_prod_ed_181.yml")
        ),
        input_ports = None
    )
    dynamicinput_33 = Process(
        name = "DynamicInput_33",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "X:\\adw\\operations\\material management\\material reconciliation\\balances\\2016-06-01.yxdb",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    perpetual_inventory__filter_183_reject = Process(
        name = "perpetual_inventory__Filter_183_reject",
        properties = ModelTransform(modelName = "perpetual_inventory__Filter_183_reject")
    )
    directory_46 = Process(
        name = "Directory_46",
        properties = Directory(
          path = "X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Balances",
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
    crosstab_81_regularactions = Process(
        name = "CrossTab_81_regularActions",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nimport pandas as pd\n\ndf = in0.copy()\ngroup_cols = [\"PartNumber\", \"Facility\", \"Plant\"]\npivot_col = \"TransactionType\"\nagg_dict = {\"Quantity\": \"sum\", \"_dummy_\": \"max\"}\ncol_to_alias = {\"Quantity\": \"Sum\", \"_dummy_\": \"_dummy_\"}\ndf[\"_dummy_\"] = 1\n\ndf_pivoted = pd.pivot_table(\n    df,\n    index=group_cols,\n    columns=pivot_col if pivot_col else None,\n    values=list(agg_dict.keys()),\n    aggfunc=agg_dict,\n)\n\nif col_to_alias:\n    if isinstance(df_pivoted.columns, pd.MultiIndex):\n        new_cols = []\n        for i in range(len(df_pivoted.columns)):\n            pivot_val = df_pivoted.columns.get_level_values(-1)[i]\n            val_col = df_pivoted.columns.get_level_values(0)[i]\n            alias = col_to_alias.get(val_col, val_col)\n            new_cols.append(f\"{pivot_val}_{alias}\")\n        df_pivoted.columns = new_cols\n    else:\n        alias = list(col_to_alias.values())[0] if col_to_alias else \"\"\n        df_pivoted = df_pivoted.rename(columns={c: f\"{c}_{alias}\" for c in df_pivoted.columns})\n\ndf_pivoted = df_pivoted.reset_index()\n\n\nreturn df_pivoted\n"
        ),
        is_custom_output_schema = True
    )
    perpetual_inventory__crosstab_81_1 = Process(
        name = "perpetual_inventory__CrossTab_81_1",
        properties = ModelTransform(modelName = "perpetual_inventory__CrossTab_81_1"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    dynamicinput_159 = Process(
        name = "DynamicInput_159",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [{"textToReplace" : "1975-12-30", "textToReplaceField" : "STARTDATE"}],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "select ITEMDESC as PartNumber,\n\tFRZCOST as UnitCost,\n\tFRZ5 + PREVFRZ5 as LaborCost,\n\tFRZ3 + PREVFRZ3 as PlatingCost,\n\tPLANNER as PlannerCode,\n\tBUYER as Buyer,\n\tORDPOL as OrderPolicy,\n\tMISCSTAT3 as CustomerSupplied,\n\tSTDCOST as StandardCost,\n\tCOST5 + PREVSTD5 as StandardLaborCost,\n\tCOST3 + PREVSTD3 as StandardPlatingCost \nfrom ODS.Analysis.tvf_bridging__dbo__PCMFG_INV('1975-12-30')",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    perpetualinvent_144 = Process(
        name = "PerpetualInvent_144",
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
            tableFullName = DatabricksTarget.WarehouseTableName(name = "PerpetualInvent_144")
          ),
          format = DatabricksTarget.DatabricksWriteFormat()
        )
    )
    test_54 = Process(
        name = "Test_54",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: Dataframe) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "assert ( in0.filter(col(\"StartDate\") >= col(\"FirstDate\")).count() > 0 )\nassert ( in0.filter(col(\"EndDate\") <= col(\"LastDate\")).count() > 0 )\nassert ( in0.filter(col(\"StartDate\") <= col(\"EndDate\")).count() > 0 )"
        )
    )
    accountingtrans_71 = Process(
        name = "AccountingTrans_71",
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
            filePath = "X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Accounting Transaction Map.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/perpetual_inventory/AccountingTrans_71.yml")
        ),
        input_ports = None
    )
    directory_57 = Process(
        name = "Directory_57",
        properties = Directory(
          path = "X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Transactions",
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
    perpetual_inventory__formula_2_0 = Process(
        name = "perpetual_inventory__Formula_2_0",
        properties = ModelTransform(modelName = "perpetual_inventory__Formula_2_0")
    )
    accountingtrans_82 = Process(
        name = "AccountingTrans_82",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Accounting Transaction Type Schema.yxdb",
              "username": "${username_AccountingTrans_82}",
              "id": "transpiled_connection",
              "port": "1521",
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = OracleSource.OracleSourceInternal(
            pathSelection = "warehouseQuery",
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "AccountingTrans_82"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Accounting Transaction Type Schema.yxdb"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/perpetual_inventory/AccountingTrans_82.yml")
        ),
        input_ports = None
    )
    textinput_1 = Process(
        name = "TextInput_1",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_1", sourceType = "Seed")
        ),
        input_ports = None
    )
    perpetual_inventory__alteryxselect_96 = Process(
        name = "perpetual_inventory__AlteryxSelect_96",
        properties = ModelTransform(modelName = "perpetual_inventory__AlteryxSelect_96")
    )
    dynamicinput_184 = Process(
        name = "DynamicInput_184",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "X:\\adw\\operations\\material management\\material reconciliation\\transactions\\2023-01-09.yxdb",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    dynamicinput_64 = Process(
        name = "DynamicInput_64",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "X:\\adw\\operations\\material management\\material reconciliation\\transactions\\2016-06-01.yxdb",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    accountingtrans_70 = Process(
        name = "AccountingTrans_70",
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
            filePath = "X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Accounting Transaction Map.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/perpetual_inventory/AccountingTrans_70.yml")
        ),
        input_ports = None
    )
    perpetual_inventory__formula_95_0 = Process(
        name = "perpetual_inventory__Formula_95_0",
        properties = ModelTransform(modelName = "perpetual_inventory__Formula_95_0")
    )
    perpetual_inventory__filter_183 = Process(
        name = "perpetual_inventory__Filter_183",
        properties = ModelTransform(modelName = "perpetual_inventory__Filter_183")
    )
    perpetual_inventory__alteryxselect_97 = Process(
        name = "perpetual_inventory__AlteryxSelect_97",
        properties = ModelTransform(modelName = "perpetual_inventory__AlteryxSelect_97")
    )
    samtecfacilitie_179 = Process(
        name = "SamtecFacilitie_179",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Samtec Facilities with Balances.yxdb",
              "username": "${username_SamtecFacilitie_179}",
              "id": "transpiled_connection",
              "port": "1521",
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = OracleSource.OracleSourceInternal(
            pathSelection = "warehouseQuery",
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "SamtecFacilitie_179"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Samtec Facilities with Balances.yxdb"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/perpetual_inventory/SamtecFacilitie_179.yml")
        ),
        input_ports = None
    )
    perpetual_inventory__alteryxselect_63 = Process(
        name = "perpetual_inventory__AlteryxSelect_63",
        properties = ModelTransform(modelName = "perpetual_inventory__AlteryxSelect_63"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    perpetual_inventory__appendfields_52 = Process(
        name = "perpetual_inventory__AppendFields_52",
        properties = ModelTransform(modelName = "perpetual_inventory__AppendFields_52"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    dynamicinput_28 = Process(
        name = "DynamicInput_28",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "X:\\adw\\operations\\material management\\material reconciliation\\balances\\2016-06-01.yxdb",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    dynamicinput_161 = Process(
        name = "DynamicInput_161",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          replaceSpecificString = [{"textToReplace" : "1975-12-30", "textToReplaceField" : "ENDDATE"}],
          tableIntegration = "oracle",
          outputMode = "",
          tableConnector = "transpiled_connection",
          passFieldsToOutput = [],
          fileType = "fileType_XLSX",
          sqlQuery = "select ITEMDESC as PartNumber,\n\tFRZCOST as UnitCost,\n\tFRZ5 + PREVFRZ5 as LaborCost,\n\tFRZ3 + PREVFRZ3 as PlatingCost,\n\tPLANNER as PlannerCode,\n\tBUYER as Buyer,\n\tORDPOL as OrderPolicy,\n\tMISCSTAT3 as CustomerSupplied,\n\tSTDCOST as StandardCost,\n\tCOST5 + PREVSTD5 as StandardLaborCost,\n\tCOST3 + PREVSTD3 as StandardPlatingCost \nfrom ODS.Analysis.tvf_bridging__dbo__PCMFG_INV('1975-12-30')",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection",
          readOptions = "modifySQLQuery",
          xlsxSheetColumn = "",
          xlsxFilePathColumn = ""
        ),
        is_custom_output_schema = True
    )
    (
        perpetual_inventory__formula_2_0._out(0)
        >> [dynamicinput_28._in(0), dynamicinput_33._in(0), perpetual_inventory__alteryxselect_63._in(1),
              perpetual_inventory__appendfields_52._in(2), perpetual_inventory__formula_95_0._in(0)]
    )
    dynamicinput_164 >> perpetual_inventory__formula_134_0._in(5)
    accountingtrans_70 >> perpetual_inventory__formula_134_0._in(2)
    dynamicinput_184 >> perpetual_inventory__crosstab_81_1._in(1)
    crosstab_81_rename >> perpetual_inventory__formula_134_0._in(11)
    samtecfacilitie_179 >> perpetual_inventory__alteryxselect_23
    perpetual_inventory__alteryxselect_96 >> dynamicinput_159
    perpetual_inventory__filter_183 >> dynamicinput_64
    accountingtrans_82 >> perpetual_inventory__formula_134_0._in(0)
    perpetual_inventory__crosstab_81_1 >> crosstab_81_regularactions
    perpetual_inventory__filter_183_reject >> dynamicinput_184
    directory_46 >> perpetual_inventory__appendfields_52._in(1)
    perpetual_inventory__formula_134_0 >> perpetualinvent_144
    perpetual_inventory__alteryxselect_97 >> dynamicinput_161
    perpetual_inventory__alteryxselect_98 >> dynamicinput_164
    perpetual_inventory__appendfields_52 >> test_54
    textinput_1 >> perpetual_inventory__formula_2_0
    dynamicinput_33 >> perpetual_inventory__formula_134_0._in(8)
    directory_57 >> perpetual_inventory__alteryxselect_63._in(0)
    dsn_r2s_prod_ed_181 >> perpetual_inventory__formula_134_0._in(6)
    dynamicinput_28 >> perpetual_inventory__formula_134_0._in(7)
    dynamicinput_161 >> perpetual_inventory__formula_134_0._in(4)
    crosstab_81_regularactions >> crosstab_81_rename
    dynamicinput_64 >> perpetual_inventory__crosstab_81_1._in(0)
    (
        perpetual_inventory__formula_95_0._out(0)
        >> [perpetual_inventory__alteryxselect_96._in(0), perpetual_inventory__alteryxselect_97._in(0),
              perpetual_inventory__alteryxselect_98._in(0)]
    )
    (
        perpetual_inventory__alteryxselect_23._out(0)
        >> [perpetual_inventory__formula_134_0._in(9), perpetual_inventory__formula_134_0._in(10),
              perpetual_inventory__crosstab_81_1._in(3)]
    )
    dynamicinput_159 >> perpetual_inventory__formula_134_0._in(3)
    (
        perpetual_inventory__alteryxselect_63._out(0)
        >> [perpetual_inventory__filter_183._in(0), perpetual_inventory__filter_183_reject._in(0)]
    )
    accountingtrans_71 >> perpetual_inventory__crosstab_81_1._in(4)
