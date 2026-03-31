from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "perpetual_inventory_details",
    version = 1,
    auto_layout = False,
    params = Parameters(
      QUESTION__TEXT_BOX_34 = "''",
      QUESTION__TEXT_BOX_110 = "''",
      USERNAME_SAMTECFACILITIE_106 = "''",
      JDBCURL_PERPETUALINVENT_105 = "''",
      PASSWORD_SAMTECFACILITIE_106 = "''",
      WORKFLOW_NAME = "'perpetual_inventory_details'",
      QUESTION__TEXT_BOX_111 = "''",
      PASSWORD_PERPETUALINVENT_105 = "''",
      QUESTION__LIST_BOX_2 = "''",
      PASSWORD_ACCOUNTINGTRANS_98 = "''",
      USERNAME_ACCOUNTINGTRANS_98 = "''",
      USERNAME_PERPETUALINVENT_105 = "''"
    )
)

with Pipeline(args) as pipeline:
    accountingtrans_98 = Process(
        name = "AccountingTrans_98",
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
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/perpetual_inventory_details/AccountingTrans_98.yml")
        ),
        input_ports = None
    )
    perpetual_inventory_details__formula_27_0 = Process(
        name = "perpetual_inventory_details__Formula_27_0",
        properties = ModelTransform(modelName = "perpetual_inventory_details__Formula_27_0")
    )
    perpetual_inventory_details__alteryxselect_62 = Process(
        name = "perpetual_inventory_details__AlteryxSelect_62",
        properties = ModelTransform(modelName = "perpetual_inventory_details__AlteryxSelect_62"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    dynamicinput_40 = Process(
        name = "DynamicInput_40",
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
    directory_11 = Process(
        name = "Directory_11",
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
    textinput_33 = Process(
        name = "TextInput_33",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_33", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_26 = Process(
        name = "TextInput_26",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_26", sourceType = "Seed")
        ),
        input_ports = None
    )
    perpetual_inventory_details__appendfields_16 = Process(
        name = "perpetual_inventory_details__AppendFields_16",
        properties = ModelTransform(modelName = "perpetual_inventory_details__AppendFields_16"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    perpetual_inventory_details__alteryxselect_103 = Process(
        name = "perpetual_inventory_details__AlteryxSelect_103",
        properties = ModelTransform(modelName = "perpetual_inventory_details__AlteryxSelect_103"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9"]
    )
    perpetual_inventory_details__alteryxselect_48 = Process(
        name = "perpetual_inventory_details__AlteryxSelect_48",
        properties = ModelTransform(modelName = "perpetual_inventory_details__AlteryxSelect_48")
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
    samtecfacilitie_106 = Process(
        name = "SamtecFacilitie_106",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "X:\\adw\\operations\\material management\\material reconciliation\\Samtec Facilities with Balances.yxdb",
              "username": "${username_SamtecFacilitie_106}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "SamtecFacilitie_106"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "X:\\adw\\operations\\material management\\material reconciliation\\Samtec Facilities with Balances.yxdb"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/perpetual_inventory_details/SamtecFacilitie_106.yml")
        ),
        input_ports = None
    )
    dynamicinput_49 = Process(
        name = "DynamicInput_49",
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
    perpetual_inventory_details__alteryxselect_39 = Process(
        name = "perpetual_inventory_details__AlteryxSelect_39",
        properties = ModelTransform(modelName = "perpetual_inventory_details__AlteryxSelect_39")
    )
    perpetual_inventory_details__formula_29_0 = Process(
        name = "perpetual_inventory_details__Formula_29_0",
        properties = ModelTransform(modelName = "perpetual_inventory_details__Formula_29_0")
    )
    test_18 = Process(
        name = "Test_18",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: Dataframe) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "assert ( in0.filter(col(\"StartDate\") >= col(\"FirstDate\")).count() > 0 )\nassert ( in0.filter(col(\"EndDate\") <= col(\"LastDate\")).count() > 0 )\nassert ( in0.filter(col(\"StartDate\") <= col(\"EndDate\")).count() > 0 )"
        )
    )
    perpetualinvent_105 = Process(
        name = "PerpetualInvent_105",
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
            tableFullName = DatabricksTarget.WarehouseTableName(name = "PerpetualInvent_105")
          ),
          format = DatabricksTarget.DatabricksWriteFormat()
        )
    )
    dynamicinput_63 = Process(
        name = "DynamicInput_63",
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
    directory_11 >> perpetual_inventory_details__appendfields_16._in(1)
    dynamicinput_49 >> perpetual_inventory_details__alteryxselect_103._in(2)
    accountingtrans_98 >> perpetual_inventory_details__alteryxselect_103._in(7)
    perpetual_inventory_details__alteryxselect_39 >> dynamicinput_40
    dynamicinput_40 >> perpetual_inventory_details__alteryxselect_103._in(1)
    dynamicinput_63 >> perpetual_inventory_details__alteryxselect_103._in(3)
    samtecfacilitie_106 >> perpetual_inventory_details__alteryxselect_103._in(0)
    (
        perpetual_inventory_details__formula_27_0._out(0)
        >> [perpetual_inventory_details__appendfields_16._in(2), perpetual_inventory_details__alteryxselect_39._in(0),
              perpetual_inventory_details__alteryxselect_48._in(0),
              perpetual_inventory_details__alteryxselect_62._in(2),
              perpetual_inventory_details__formula_29_0._in(0)]
    )
    textinput_33 >> perpetual_inventory_details__alteryxselect_103._in(8)
    perpetual_inventory_details__appendfields_16 >> test_18
    textinput_26 >> perpetual_inventory_details__formula_27_0
    directory_57 >> perpetual_inventory_details__alteryxselect_62._in(0)
    perpetual_inventory_details__alteryxselect_62 >> dynamicinput_63
    perpetual_inventory_details__alteryxselect_103 >> perpetualinvent_105
    perpetual_inventory_details__alteryxselect_48 >> dynamicinput_49
