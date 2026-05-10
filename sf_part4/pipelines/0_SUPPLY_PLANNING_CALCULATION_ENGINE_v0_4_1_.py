from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_",
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
      USERNAME_TARGETQUAN_XLSX_620 = "''",
      PASSWORD_TARGETQUAN_XLSX_620 = "''",
      JDBCURL_RGF_INV_AVAILAB_421 = "''",
      USERNAME_RGF_INV_AVAILAB_421 = "''",
      PASSWORD_RGF_INV_AVAILAB_421 = "''",
      JDBCURL_RGF_INV_AVAILAB_385 = "''",
      USERNAME_RGF_INV_AVAILAB_385 = "''",
      PASSWORD_RGF_INV_AVAILAB_385 = "''",
      JDBCURL_RGF_INV_AVAILAB_383 = "''",
      USERNAME_RGF_INV_AVAILAB_383 = "''",
      PASSWORD_RGF_INV_AVAILAB_383 = "''",
      WORKFLOW_NAME = "'0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_'",
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
    node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_228 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_228",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_228")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_338 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_338",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_338"),
        input_ports = ["in_0", "in_1"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_372 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_372",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_372")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_387 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_387",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_387")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_422",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_422"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14", "in_15", "in_16", "in_17", "in_18"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_533 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_533",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_533")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_547 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_547",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_547")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_551 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_551",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_551"),
        input_ports = ["in_0", "in_1"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___cleanse_214 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Cleanse_214",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Cleanse_214")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___cleanse_316 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Cleanse_316",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Cleanse_316")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___cleanse_317 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Cleanse_317",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Cleanse_317"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___filter_480 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Filter_480",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Filter_480")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___findreplace_369_join = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___FindReplace_369_join",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___FindReplace_369_join"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___findreplace_370_join = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___FindReplace_370_join",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___FindReplace_370_join"),
        input_ports = ["in_0", "in_1"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___findreplace_390_join = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___FindReplace_390_join",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___FindReplace_390_join"),
        input_ports = ["in_0", "in_1"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___formula_215_0 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_215_0",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_215_0")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___formula_237_0 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_237_0",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_237_0")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___formula_282_0 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_282_0",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_282_0"),
        input_ports = ["in_0", "in_1"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___formula_347_1 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_347_1",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_347_1"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___formula_350_0 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_350_0",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_350_0")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___join_195_left = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Join_195_left",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Join_195_left"),
        input_ports = ["in_0", "in_1"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___join_202_left = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Join_202_left",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Join_202_left"),
        input_ports = ["in_0", "in_1"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___join_339_left = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Join_339_left",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Join_339_left"),
        input_ports = ["in_0", "in_1"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___selectrecords_386_cleanup_0 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___SelectRecords_386_cleanup_0",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___SelectRecords_386_cleanup_0")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___summarize_173 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Summarize_173",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Summarize_173")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___summarize_355 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Summarize_355",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Summarize_355")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___summarize_626 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Summarize_626",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Summarize_626")
    )
    node_0_supply_planning_calculation_engine_v0_4_1___union_160 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Union_160",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Union_160"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___union_198 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Union_198",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Union_198"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___union_449 = Process(
        name = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Union_449",
        properties = ModelTransform(modelName = "0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Union_449"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/DbFileInput_127_127.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/DbFileInput_254_254.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/DbFileInput_255_255.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/DbFileInput_265_265.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/DbFileInput_297_297.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/DbFileInput_35_35.yml"
          )
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
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/DbFileInput_503_503.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/DbFileInput_572_572.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/DbFileInput_8_8.yml"
          )
        ),
        input_ports = None
    )
    findreplace_369 = Process(
        name = "FindReplace_369",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nimport re\nimport pandas as pd\nimport json\n\nfind_col = \"SKU_STANDARD\"\nbase_col = \"SKU_STANDARD\"\nflags = 0\n\ndef wrap_pat(pat):\n    p = str(pat)\n    if False:\n        p = r\"\\b\" + p + r\"\\b\"\n    if False:\n        p = \"^\" + p\n    elif False:\n        p = \"^\" + p + \"$\"\n    return p\n\ndef first_matching_rule(text, rules):\n    if text is None or (isinstance(text, float) and pd.isna(text)):\n        return \"{}\"\n    text = str(text)\n    for rule in rules:\n        if rule is None or not isinstance(rule, dict):\n            continue\n        pat = rule.get(find_col) or rule.get(find_col.replace(\"`\", \"\"))\n        if pat is None:\n            continue\n        pat = wrap_pat(pat)\n        if re.search(pat, text, flags=flags):\n            return json.dumps(rule)\n    return \"{}\"\n\nout0 = in0.copy()\nrules_col = out0[\"_rules\"]\nextracted = [\n    first_matching_rule(row[base_col], row[rules_col] if isinstance(row[rules_col], list) else [])\n    for _, row in out0.iterrows()\n]\nout0[\"_extracted_rule\"] = extracted\n"
        ),
        is_custom_output_schema = True
    )
    findreplace_370 = Process(
        name = "FindReplace_370",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nimport re\nimport pandas as pd\nimport json\n\nfind_col = \"WH_ID_STANDARD\"\nbase_col = \"WH_ID_STANDARD\"\nflags = 0\n\ndef wrap_pat(pat):\n    p = str(pat)\n    if False:\n        p = r\"\\b\" + p + r\"\\b\"\n    if False:\n        p = \"^\" + p\n    elif False:\n        p = \"^\" + p + \"$\"\n    return p\n\ndef first_matching_rule(text, rules):\n    if text is None or (isinstance(text, float) and pd.isna(text)):\n        return \"{}\"\n    text = str(text)\n    for rule in rules:\n        if rule is None or not isinstance(rule, dict):\n            continue\n        pat = rule.get(find_col) or rule.get(find_col.replace(\"`\", \"\"))\n        if pat is None:\n            continue\n        pat = wrap_pat(pat)\n        if re.search(pat, text, flags=flags):\n            return json.dumps(rule)\n    return \"{}\"\n\nout0 = in0.copy()\nrules_col = out0[\"_rules\"]\nextracted = [\n    first_matching_rule(row[base_col], row[rules_col] if isinstance(row[rules_col], list) else [])\n    for _, row in out0.iterrows()\n]\nout0[\"_extracted_rule\"] = extracted\n"
        ),
        is_custom_output_schema = True
    )
    findreplace_390 = Process(
        name = "FindReplace_390",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\nimport re\nimport pandas as pd\nimport json\n\nfind_col = \"ROWTYPE\"\nbase_col = \"ROWTYPE\"\nflags = 0\n\ndef wrap_pat(pat):\n    p = str(pat)\n    if False:\n        p = r\"\\b\" + p + r\"\\b\"\n    if False:\n        p = \"^\" + p\n    elif False:\n        p = \"^\" + p + \"$\"\n    return p\n\ndef first_matching_rule(text, rules):\n    if text is None or (isinstance(text, float) and pd.isna(text)):\n        return \"{}\"\n    text = str(text)\n    for rule in rules:\n        if rule is None or not isinstance(rule, dict):\n            continue\n        pat = rule.get(find_col) or rule.get(find_col.replace(\"`\", \"\"))\n        if pat is None:\n            continue\n        pat = wrap_pat(pat)\n        if re.search(pat, text, flags=flags):\n            return json.dumps(rule)\n    return \"{}\"\n\nout0 = in0.copy()\nrules_col = out0[\"_rules\"]\nextracted = [\n    first_matching_rule(row[base_col], row[rules_col] if isinstance(row[rules_col], list) else [])\n    for _, row in out0.iterrows()\n]\nout0[\"_extracted_rule\"] = extracted\n"
        ),
        is_custom_output_schema = True
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
          format = SFTPSource.CsvReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/Lineage11_01_21_570.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/RLS20211129_001_615.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/SalesForecastMa_532.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/SalesQB11_10_21_264.yml"
          )
        ),
        input_ports = None
    )
    targetquan_xlsx_620 = Process(
        name = "TargetQuan_xlsx_620",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "TargetQuan.xlsx"),
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/TargetQuan_xlsx_620.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/WOH120921_xlsx__476.yml"
          )
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
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1_/WarehouseMaster_129.yml"
          )
        ),
        input_ports = None
    )
    (
        node_0_supply_planning_calculation_engine_v0_4_1___union_160._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___cleanse_317._in(0),
              node_0_supply_planning_calculation_engine_v0_4_1___summarize_173._in(0)]
    )
    salesforecastma_532 >> node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_533
    node_0_supply_planning_calculation_engine_v0_4_1___findreplace_390_join >> findreplace_390
    node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_387 >> rgf_inv_availab_383
    dbfileinput_8_8 >> node_0_supply_planning_calculation_engine_v0_4_1___cleanse_214
    dbfileinput_254_254 >> node_0_supply_planning_calculation_engine_v0_4_1___union_160._in(3)
    (
        node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_533._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(18),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_547._in(0)]
    )
    (
        node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_551._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___findreplace_369_join._in(2),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(13)]
    )
    dbfileinput_127_127 >> node_0_supply_planning_calculation_engine_v0_4_1___union_160._in(5)
    findreplace_369 >> node_0_supply_planning_calculation_engine_v0_4_1___findreplace_370_join._in(1)
    node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422 >> rgf_inv_availab_421
    (
        node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_547._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(3),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_551._in(0)]
    )
    salesqb11_10_21_264 >> node_0_supply_planning_calculation_engine_v0_4_1___formula_282_0._in(1)
    (
        node_0_supply_planning_calculation_engine_v0_4_1___union_449._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(8),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(15),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_228._in(0)]
    )
    (
        node_0_supply_planning_calculation_engine_v0_4_1___join_339_left._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(17),
              node_0_supply_planning_calculation_engine_v0_4_1___formula_347_1._in(0)]
    )
    (
        node_0_supply_planning_calculation_engine_v0_4_1___union_198._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___union_449._in(1),
              node_0_supply_planning_calculation_engine_v0_4_1___join_202_left._in(1)]
    )
    (
        node_0_supply_planning_calculation_engine_v0_4_1___formula_350_0._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___findreplace_369_join._in(5),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(6)]
    )
    targetquan_xlsx_620 >> node_0_supply_planning_calculation_engine_v0_4_1___summarize_626
    textinput_388 >> node_0_supply_planning_calculation_engine_v0_4_1___findreplace_390_join._in(0)
    (
        node_0_supply_planning_calculation_engine_v0_4_1___join_195_left._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(1),
              node_0_supply_planning_calculation_engine_v0_4_1___union_198._in(2)]
    )
    (
        node_0_supply_planning_calculation_engine_v0_4_1___summarize_355._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___findreplace_369_join._in(4),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(9)]
    )
    findreplace_370 >> node_0_supply_planning_calculation_engine_v0_4_1___findreplace_390_join._in(1)
    (
        node_0_supply_planning_calculation_engine_v0_4_1___formula_282_0._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(2),
              node_0_supply_planning_calculation_engine_v0_4_1___cleanse_317._in(2)]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___findreplace_369_join >> findreplace_369
    (
        node_0_supply_planning_calculation_engine_v0_4_1___formula_215_0._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___findreplace_369_join._in(1),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(0),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(11),
              node_0_supply_planning_calculation_engine_v0_4_1___union_449._in(2),
              node_0_supply_planning_calculation_engine_v0_4_1___join_202_left._in(0),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_338._in(1),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_551._in(1)]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___selectrecords_386_cleanup_0 >> rgf_inv_availab_385
    (
        node_0_supply_planning_calculation_engine_v0_4_1___formula_237_0._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(14),
              node_0_supply_planning_calculation_engine_v0_4_1___formula_347_1._in(2),
              node_0_supply_planning_calculation_engine_v0_4_1___join_339_left._in(1)]
    )
    (
        node_0_supply_planning_calculation_engine_v0_4_1___cleanse_317._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___union_198._in(1),
              node_0_supply_planning_calculation_engine_v0_4_1___join_195_left._in(1)]
    )
    lineage11_01_21_570 >> node_0_supply_planning_calculation_engine_v0_4_1___union_160._in(1)
    rls20211129_001_615 >> node_0_supply_planning_calculation_engine_v0_4_1___union_160._in(7)
    (
        node_0_supply_planning_calculation_engine_v0_4_1___cleanse_214._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(7),
              node_0_supply_planning_calculation_engine_v0_4_1___formula_215_0._in(0)]
    )
    node_0_supply_planning_calculation_engine_v0_4_1___findreplace_370_join >> findreplace_370
    (
        node_0_supply_planning_calculation_engine_v0_4_1___filter_480._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(12),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_338._in(0)]
    )
    (
        node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_338._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(5),
              node_0_supply_planning_calculation_engine_v0_4_1___formula_347_1._in(1),
              node_0_supply_planning_calculation_engine_v0_4_1___join_339_left._in(0)]
    )
    (
        node_0_supply_planning_calculation_engine_v0_4_1___formula_347_1._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___findreplace_369_join._in(6),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(16),
              node_0_supply_planning_calculation_engine_v0_4_1___formula_350_0._in(0)]
    )
    woh120921_xlsx__476 >> node_0_supply_planning_calculation_engine_v0_4_1___filter_480
    dbfileinput_265_265 >> node_0_supply_planning_calculation_engine_v0_4_1___formula_282_0._in(0)
    (
        dbfileinput_503_503._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___union_160._in(2),
              node_0_supply_planning_calculation_engine_v0_4_1___union_160._in(4)]
    )
    findreplace_390 >> node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_372
    dbfileinput_297_297 >> node_0_supply_planning_calculation_engine_v0_4_1___cleanse_317._in(1)
    (
        node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_228._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___findreplace_369_join._in(0),
              node_0_supply_planning_calculation_engine_v0_4_1___findreplace_369_join._in(3),
              node_0_supply_planning_calculation_engine_v0_4_1___summarize_355._in(0),
              node_0_supply_planning_calculation_engine_v0_4_1___formula_237_0._in(0)]
    )
    (
        node_0_supply_planning_calculation_engine_v0_4_1___join_202_left._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(4),
              node_0_supply_planning_calculation_engine_v0_4_1___union_449._in(0)]
    )
    dbfileinput_572_572 >> node_0_supply_planning_calculation_engine_v0_4_1___union_160._in(6)
    (
        node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_372._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___selectrecords_386_cleanup_0._in(0),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_387._in(0)]
    )
    (
        node_0_supply_planning_calculation_engine_v0_4_1___cleanse_316._out(0)
        >> [node_0_supply_planning_calculation_engine_v0_4_1___findreplace_370_join._in(0),
              node_0_supply_planning_calculation_engine_v0_4_1___alteryxselect_422._in(10),
              node_0_supply_planning_calculation_engine_v0_4_1___union_198._in(0),
              node_0_supply_planning_calculation_engine_v0_4_1___join_195_left._in(0)]
    )
    dbfileinput_35_35 >> node_0_supply_planning_calculation_engine_v0_4_1___union_160._in(0)
    warehousemaster_129 >> node_0_supply_planning_calculation_engine_v0_4_1___cleanse_316
    dbfileinput_255_255 >> node_0_supply_planning_calculation_engine_v0_4_1___union_160._in(8)
