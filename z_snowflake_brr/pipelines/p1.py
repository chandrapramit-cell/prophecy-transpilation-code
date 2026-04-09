from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "p1",
    version = 1,
    auto_layout = False,
    params = Parameters(
      AMC_COMBINED = "''",
      ITEM_MASTER_LOOKUP = "''",
      WAREHOUSE_MASTER_LOOKUP = "''",
      HANSON = "''",
      IWI = "''",
      CITY_OF_INDUSTRY = "''",
      ESTIMATES_BY_CUSTOMER_QBO = "''",
      SALES_BY_CUSTOMER_DETAIL_QBO = "''",
      OPEN_PO_QBO = "''",
      WEEKLY_AVERAGE_SALES_RECAP_TAB_SALES_CASES_WK_DONT_CHG_FORMAT = "''",
      DAVY = "''",
      LINEAGE = "''",
      SALES_FORECAST_MASTER_MONTHLY = "''",
      SSRE_ANA___TRADER_JOES = "''",
      RLS_INVENTORY_REPORT = "''",
      USERNAME_DBFILEINPUT_503_503 = "''",
      PASSWORD_DBFILEINPUT_503_503 = "''",
      USERNAME_DBFILEINPUT_572_572 = "''",
      PASSWORD_DBFILEINPUT_572_572 = "''",
      USERNAME_DBFILEINPUT_35_35 = "''",
      PASSWORD_DBFILEINPUT_35_35 = "''",
      USERNAME_DBFILEINPUT_127_127 = "''",
      PASSWORD_DBFILEINPUT_127_127 = "''",
      USERNAME_DBFILEINPUT_254_254 = "''",
      PASSWORD_DBFILEINPUT_254_254 = "''",
      USERNAME_DBFILEINPUT_255_255 = "''",
      PASSWORD_DBFILEINPUT_255_255 = "''",
      USERNAME_RLS20211129_001_615 = "''",
      PASSWORD_RLS20211129_001_615 = "''",
      USERNAME_DBFILEINPUT_265_265 = "''",
      PASSWORD_DBFILEINPUT_265_265 = "''",
      USERNAME_SALESQB11_10_21_264 = "''",
      PASSWORD_SALESQB11_10_21_264 = "''",
      USERNAME_DBFILEINPUT_297_297 = "''",
      PASSWORD_DBFILEINPUT_297_297 = "''",
      USERNAME_WAREHOUSEMASTER_129 = "''",
      PASSWORD_WAREHOUSEMASTER_129 = "''",
      USERNAME_DBFILEINPUT_8_8 = "''",
      PASSWORD_DBFILEINPUT_8_8 = "''",
      USERNAME_WOH120921_XLSX__476 = "''",
      PASSWORD_WOH120921_XLSX__476 = "''",
      USERNAME_SALESFORECASTMA_532 = "''",
      PASSWORD_SALESFORECASTMA_532 = "''",
      JDBCURL_RGF_INV_AVAILAB_421 = "''",
      USERNAME_RGF_INV_AVAILAB_421 = "''",
      PASSWORD_RGF_INV_AVAILAB_421 = "''",
      JDBCURL_RGF_INV_AVAILAB_385 = "''",
      USERNAME_RGF_INV_AVAILAB_385 = "''",
      PASSWORD_RGF_INV_AVAILAB_385 = "''",
      JDBCURL_RGF_INV_AVAILAB_383 = "''",
      USERNAME_RGF_INV_AVAILAB_383 = "''",
      PASSWORD_RGF_INV_AVAILAB_383 = "''",
      WORKFLOW_NAME = "'test'",
      QUESTION__FILE_BROWSE_450 = "''",
      QUESTION__FILE_BROWSE_452 = "''",
      QUESTION__FILE_BROWSE_446 = "''",
      QUESTION__FILE_BROWSE_454 = "''",
      QUESTION__FILE_BROWSE_456 = "''",
      QUESTION__FILE_BROWSE_458 = "''",
      QUESTION__FILE_BROWSE_528 = "''",
      QUESTION__FILE_BROWSE_530 = "''",
      QUESTION__FILE_BROWSE_460 = "''",
      QUESTION__FILE_BROWSE_462 = "''",
      QUESTION__FILE_BROWSE_464 = "''",
      QUESTION__FILE_BROWSE_487 = "''",
      QUESTION__FILE_BROWSE_567 = "''",
      QUESTION__FILE_BROWSE_606 = "''",
      QUESTION__FILE_BROWSE_618 = "''"
    )
)

with Pipeline(args) as pipeline:

    with visual_group("DataQualityIssues"):
        pass

    with visual_group("INPUTITEMMASTER"):
        pass

    dbfileinput_127_127 = Process(
        name = "DbFileInput_127_127",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "12.13.21 AMC.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/DbFileInput_127_127.yml")
        ),
        input_ports = None
    )
    dbfileinput_254_254 = Process(
        name = "DbFileInput_254_254",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "12.13.21 IWI.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/DbFileInput_254_254.yml")
        ),
        input_ports = None
    )
    dbfileinput_255_255 = Process(
        name = "DbFileInput_255_255",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "12.13.21 COI.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/DbFileInput_255_255.yml")
        ),
        input_ports = None
    )
    dbfileinput_265_265 = Process(
        name = "DbFileInput_265_265",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "12.13.21 Estimates.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/DbFileInput_265_265.yml")
        ),
        input_ports = None
    )
    dbfileinput_297_297 = Process(
        name = "DbFileInput_297_297",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "12.13.21 PO's.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/DbFileInput_297_297.yml")
        ),
        input_ports = None
    )
    dbfileinput_35_35 = Process(
        name = "DbFileInput_35_35",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "12.13.21 Hanson.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/DbFileInput_35_35.yml")
        ),
        input_ports = None
    )
    dbfileinput_503_503 = Process(
        name = "DbFileInput_503_503",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "12.13.21 Davy.xls",
              "username": "${username_DbFileInput_503_503}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "DbFileInput_503_503"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "`DetailedInventoryByOwnerReport$`")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/p1/DbFileInput_503_503.yml")
        ),
        input_ports = None
    )
    dbfileinput_572_572 = Process(
        name = "DbFileInput_572_572",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "12.13.21 SSRE.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/DbFileInput_572_572.yml")
        ),
        input_ports = None
    )
    dbfileinput_8_8 = Process(
        name = "DbFileInput_8_8",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "12.10.21 Item Master v2.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/DbFileInput_8_8.yml")
        ),
        input_ports = None
    )
    lineage11_01_21_570 = Process(
        name = "Lineage11_01_21_570",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "Lineage 11.01.21.csv"),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/p1/Lineage11_01_21_570.yml")
        ),
        input_ports = None
    )
    rgf_inv_availab_383 = Process(
        name = "RGF_INV_AVAILAB_383",
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
            tableFullName = DatabricksTarget.WarehouseTableName(name = "RGF_INV_AVAILAB_383")
          ),
          format = DatabricksTarget.DatabricksWriteFormat()
        )
    )
    rgf_inv_availab_385 = Process(
        name = "RGF_INV_AVAILAB_385",
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
            tableFullName = DatabricksTarget.WarehouseTableName(name = "RGF_INV_AVAILAB_385")
          ),
          format = DatabricksTarget.DatabricksWriteFormat()
        )
    )
    rgf_inv_availab_421 = Process(
        name = "RGF_INV_AVAILAB_421",
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
            tableFullName = DatabricksTarget.WarehouseTableName(name = "RGF_INV_AVAILAB_421")
          ),
          format = DatabricksTarget.DatabricksWriteFormat()
        )
    )
    rls20211129_001_615 = Process(
        name = "RLS20211129_001_615",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "RLS 20211129_001.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/RLS20211129_001_615.yml")
        ),
        input_ports = None
    )
    salesforecastma_532 = Process(
        name = "SalesForecastMa_532",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "Sales Forecast Master 09-22-21.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/SalesForecastMa_532.yml")
        ),
        input_ports = None
    )
    salesqb11_10_21_264 = Process(
        name = "SalesQB11_10_21_264",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "Sales QB 11.10.21.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/SalesQB11_10_21_264.yml")
        ),
        input_ports = None
    )
    textinput_388 = Process(
        name = "TextInput_388",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_388", sourceType = "Seed")
        ),
        input_ports = None
    )
    woh120921_xlsx__476 = Process(
        name = "WOH120921_xlsx__476",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "WOH 120921.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/WOH120921_xlsx__476.yml")
        ),
        input_ports = None
    )
    warehousemaster_129 = Process(
        name = "WarehouseMaster_129",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "Warehouse Master (4).xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/p1/WarehouseMaster_129.yml")
        ),
        input_ports = None
    )
    p1__alteryxselect_228 = Process(
        name = "p1__AlteryxSelect_228",
        properties = ModelTransform(modelName = "p1__AlteryxSelect_228")
    )
    p1__alteryxselect_338 = Process(
        name = "p1__AlteryxSelect_338",
        properties = ModelTransform(modelName = "p1__AlteryxSelect_338"),
        input_ports = ["in_0", "in_1"]
    )
    p1__alteryxselect_372 = Process(
        name = "p1__AlteryxSelect_372",
        properties = ModelTransform(modelName = "p1__AlteryxSelect_372"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8"]
    )
    p1__alteryxselect_387 = Process(
        name = "p1__AlteryxSelect_387",
        properties = ModelTransform(modelName = "p1__AlteryxSelect_387")
    )
    p1__alteryxselect_422 = Process(
        name = "p1__AlteryxSelect_422",
        properties = ModelTransform(modelName = "p1__AlteryxSelect_422"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14", "in_15", "in_16", "in_17", "in_18"]
    )
    p1__alteryxselect_533 = Process(
        name = "p1__AlteryxSelect_533",
        properties = ModelTransform(modelName = "p1__AlteryxSelect_533")
    )
    p1__alteryxselect_547 = Process(
        name = "p1__AlteryxSelect_547",
        properties = ModelTransform(modelName = "p1__AlteryxSelect_547")
    )
    p1__alteryxselect_551 = Process(
        name = "p1__AlteryxSelect_551",
        properties = ModelTransform(modelName = "p1__AlteryxSelect_551"),
        input_ports = ["in_0", "in_1"]
    )
    p1__cleanse_214 = Process(name = "p1__Cleanse_214", properties = ModelTransform(modelName = "p1__Cleanse_214"))
    p1__cleanse_316 = Process(name = "p1__Cleanse_316", properties = ModelTransform(modelName = "p1__Cleanse_316"))
    p1__cleanse_317 = Process(
        name = "p1__Cleanse_317",
        properties = ModelTransform(modelName = "p1__Cleanse_317"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10"]
    )
    p1__filter_480 = Process(name = "p1__Filter_480", properties = ModelTransform(modelName = "p1__Filter_480"))
    p1__formula_215_0 = Process(name = "p1__Formula_215_0", properties = ModelTransform(modelName = "p1__Formula_215_0"))
    p1__formula_237_0 = Process(name = "p1__Formula_237_0", properties = ModelTransform(modelName = "p1__Formula_237_0"))
    p1__formula_282_0 = Process(
        name = "p1__Formula_282_0",
        properties = ModelTransform(modelName = "p1__Formula_282_0"),
        input_ports = ["in_0", "in_1"]
    )
    p1__formula_347_1 = Process(
        name = "p1__Formula_347_1",
        properties = ModelTransform(modelName = "p1__Formula_347_1"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    p1__formula_350_0 = Process(name = "p1__Formula_350_0", properties = ModelTransform(modelName = "p1__Formula_350_0"))
    p1__join_195_left = Process(
        name = "p1__Join_195_left",
        properties = ModelTransform(modelName = "p1__Join_195_left"),
        input_ports = ["in_0", "in_1"]
    )
    p1__join_202_left = Process(
        name = "p1__Join_202_left",
        properties = ModelTransform(modelName = "p1__Join_202_left"),
        input_ports = ["in_0", "in_1"]
    )
    p1__join_339_left = Process(
        name = "p1__Join_339_left",
        properties = ModelTransform(modelName = "p1__Join_339_left"),
        input_ports = ["in_0", "in_1"]
    )
    p1__selectrecords_386_cleanup_0 = Process(
        name = "p1__SelectRecords_386_cleanup_0",
        properties = ModelTransform(modelName = "p1__SelectRecords_386_cleanup_0")
    )
    p1__summarize_355 = Process(name = "p1__Summarize_355", properties = ModelTransform(modelName = "p1__Summarize_355"))
    p1__union_198 = Process(
        name = "p1__Union_198",
        properties = ModelTransform(modelName = "p1__Union_198"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    p1__union_449 = Process(
        name = "p1__Union_449",
        properties = ModelTransform(modelName = "p1__Union_449"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    p1__selectrecords_386_cleanup_0 >> rgf_inv_availab_385
    salesforecastma_532 >> p1__alteryxselect_533
    (
        p1__formula_215_0._out(0)
        >> [p1__alteryxselect_422._in(5), p1__alteryxselect_422._in(18), p1__alteryxselect_372._in(1),
              p1__union_449._in(0), p1__join_202_left._in(0), p1__alteryxselect_338._in(1),
              p1__alteryxselect_551._in(0)]
    )
    p1__alteryxselect_387 >> rgf_inv_availab_383
    dbfileinput_8_8 >> p1__cleanse_214
    p1__formula_282_0._out(0) >> [p1__alteryxselect_422._in(15), p1__cleanse_317._in(7)]
    dbfileinput_254_254 >> p1__cleanse_317._in(3)
    p1__alteryxselect_533._out(0) >> [p1__alteryxselect_422._in(12), p1__alteryxselect_547._in(0)]
    p1__alteryxselect_551._out(0) >> [p1__alteryxselect_422._in(3), p1__alteryxselect_372._in(4)]
    dbfileinput_127_127 >> p1__cleanse_317._in(6)
    p1__alteryxselect_422 >> rgf_inv_availab_421
    p1__alteryxselect_547._out(0) >> [p1__alteryxselect_422._in(0), p1__alteryxselect_551._in(1)]
    p1__formula_350_0._out(0) >> [p1__alteryxselect_422._in(11), p1__alteryxselect_372._in(7)]
    salesqb11_10_21_264 >> p1__formula_282_0._in(1)
    p1__union_449._out(0) >> [p1__alteryxselect_422._in(1), p1__alteryxselect_422._in(16), p1__alteryxselect_228._in(0)]
    p1__union_198._out(0) >> [p1__union_449._in(1), p1__join_202_left._in(1)]
    p1__join_195_left._out(0) >> [p1__alteryxselect_422._in(8), p1__union_198._in(0)]
    textinput_388 >> p1__alteryxselect_372._in(2)
    p1__summarize_355._out(0) >> [p1__alteryxselect_422._in(4), p1__alteryxselect_372._in(0)]
    p1__cleanse_317._out(0) >> [p1__union_198._in(1), p1__join_195_left._in(1)]
    lineage11_01_21_570 >> p1__cleanse_317._in(1)
    rls20211129_001_615 >> p1__cleanse_317._in(10)
    p1__formula_237_0._out(0) >> [p1__alteryxselect_422._in(7), p1__formula_347_1._in(0), p1__join_339_left._in(1)]
    p1__cleanse_214._out(0) >> [p1__alteryxselect_422._in(9), p1__formula_215_0._in(0)]
    p1__join_202_left._out(0) >> [p1__alteryxselect_422._in(14), p1__union_449._in(2)]
    p1__filter_480._out(0) >> [p1__alteryxselect_422._in(10), p1__alteryxselect_338._in(0)]
    p1__join_339_left._out(0) >> [p1__alteryxselect_422._in(13), p1__formula_347_1._in(1)]
    p1__alteryxselect_338._out(0) >> [p1__alteryxselect_422._in(2), p1__formula_347_1._in(2), p1__join_339_left._in(0)]
    p1__formula_347_1._out(0) >> [p1__alteryxselect_422._in(6), p1__alteryxselect_372._in(6), p1__formula_350_0._in(0)]
    woh120921_xlsx__476 >> p1__filter_480
    dbfileinput_265_265 >> p1__formula_282_0._in(0)
    dbfileinput_503_503._out(0) >> [p1__cleanse_317._in(2), p1__cleanse_317._in(4)]
    dbfileinput_297_297 >> p1__cleanse_317._in(5)
    (
        p1__alteryxselect_228._out(0)
        >> [p1__alteryxselect_372._in(3), p1__alteryxselect_372._in(5), p1__summarize_355._in(0),
              p1__formula_237_0._in(0)]
    )
    dbfileinput_572_572 >> p1__cleanse_317._in(8)
    p1__alteryxselect_372._out(0) >> [p1__selectrecords_386_cleanup_0._in(0), p1__alteryxselect_387._in(0)]
    (
        p1__cleanse_316._out(0)
        >> [p1__alteryxselect_422._in(17), p1__alteryxselect_372._in(8), p1__union_198._in(2),
              p1__join_195_left._in(0)]
    )
    dbfileinput_35_35 >> p1__cleanse_317._in(0)
    warehousemaster_129 >> p1__cleanse_316
    dbfileinput_255_255 >> p1__cleanse_317._in(9)
