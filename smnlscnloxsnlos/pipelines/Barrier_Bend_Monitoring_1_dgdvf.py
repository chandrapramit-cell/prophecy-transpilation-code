from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Barrier_Bend_Monitoring_1_dgdvf",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_BarrierBendingM_5 = "''",
      password_BarrierBendingM_5 = "''",
      username_SmartBendingRep_64 = "''",
      password_SmartBendingRep_64 = "''",
      username_SmartBendingRep_67 = "''",
      password_SmartBendingRep_67 = "''",
      jdbcUrl_BarrierBendingM_73 = "''",
      username_BarrierBendingM_73 = "''",
      password_BarrierBendingM_73 = "''",
      username_SmartBendingRep_79 = "''",
      password_SmartBendingRep_79 = "''",
      jdbcUrl_BarrierBendingM_102 = "''",
      username_BarrierBendingM_102 = "''",
      password_BarrierBendingM_102 = "''",
      jdbcUrl_BarrierBendingM_74 = "''",
      username_BarrierBendingM_74 = "''",
      password_BarrierBendingM_74 = "''",
      workflow_name = "'Barrier_Bend_Monitoring_1_dgdvf'"
    )
)

with Pipeline(args) as pipeline:
    barreport_apac__1 = Process(
        name = "BarReport_APAC__1",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Input files BB\\BarReport_APAC*.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/Barrier_Bend_Monitoring_1_dgdvf/BarReport_APAC__1.yml"
          )
        ),
        input_ports = None
    )
    barrierbendingm_102 = Process(
        name = "BarrierBendingM_102",
        properties = SFTPTarget(
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
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Output files BB\\Barrier Bending  Monitoring -APAC.xlsx"
          ),
          format = SFTPTarget.XLSXWriteFormat()
        )
    )
    barrierbendingm_5 = Process(
        name = "BarrierBendingM_5",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Input files BB\\Barrier Bending  Monitoring -APAC.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Barrier_Bend_Monitoring_1_dgdvf/BarrierBendingM_5.yml",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    barrierbendingm_73 = Process(
        name = "BarrierBendingM_73",
        properties = SFTPTarget(
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
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Output files BB\\Barrier Bending  Monitoring -APAC.xlsx"
          ),
          format = SFTPTarget.XLSXWriteFormat()
        )
    )
    barrierbendingm_74 = Process(
        name = "BarrierBendingM_74",
        properties = SFTPTarget(
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
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Output files BB\\Barrier Bending  Monitoring -APAC.xlsx"
          ),
          format = SFTPTarget.XLSXWriteFormat()
        )
    )
    barrier_bend_monitoring_1_dgdvf__alteryxselect_80 = Process(
        name = "Barrier_Bend_Monitoring_1_dgdvf__AlteryxSelect_80",
        properties = ModelTransform(modelName = "Barrier_Bend_Monitoring_1_dgdvf__AlteryxSelect_80")
    )
    barrier_bend_monitoring_1_dgdvf__alteryxselect_84 = Process(
        name = "Barrier_Bend_Monitoring_1_dgdvf__AlteryxSelect_84",
        properties = ModelTransform(modelName = "Barrier_Bend_Monitoring_1_dgdvf__AlteryxSelect_84"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    barrier_bend_monitoring_1_dgdvf__formula_72_3 = Process(
        name = "Barrier_Bend_Monitoring_1_dgdvf__Formula_72_3",
        properties = ModelTransform(modelName = "Barrier_Bend_Monitoring_1_dgdvf__Formula_72_3"),
        input_ports = ["in_0", "in_1"]
    )
    barrier_bend_monitoring_1_dgdvf__formula_85_0 = Process(
        name = "Barrier_Bend_Monitoring_1_dgdvf__Formula_85_0",
        properties = ModelTransform(modelName = "Barrier_Bend_Monitoring_1_dgdvf__Formula_85_0"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    portfoliocomposertable_98 = Process(name = "PortfolioComposerTable_98", properties = Visualize(), output_ports = None)
    smartbendingrep_64 = Process(
        name = "SmartBendingRep_64",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Input files BB\\SmartBendingReport*.xls",
              "username": "${username_SmartBendingRep_64}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "SmartBendingRep_64"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "`ASSET_FILTERING$`")
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Barrier_Bend_Monitoring_1_dgdvf/SmartBendingRep_64.yml"
          )
        ),
        input_ports = None
    )
    smartbendingrep_67 = Process(
        name = "SmartBendingRep_67",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Input files BB\\SmartBendingReport*.xls",
              "username": "${username_SmartBendingRep_67}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "SmartBendingRep_67"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "`PUT2FWD_NOKO$`")
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Barrier_Bend_Monitoring_1_dgdvf/SmartBendingRep_67.yml"
          )
        ),
        input_ports = None
    )
    smartbendingrep_79 = Process(
        name = "SmartBendingRep_79",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Input files BB\\SmartBendingReport*.xls",
              "username": "${username_SmartBendingRep_79}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "SmartBendingRep_79"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "`GUARATEE_KO$`")
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Barrier_Bend_Monitoring_1_dgdvf/SmartBendingRep_79.yml"
          )
        ),
        input_ports = None
    )
    textinput_9 = Process(
        name = "TextInput_9",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_Barrier_Bend_Monitoring_1_dgdvf_9", sourceType = "Seed")
        ),
        input_ports = None
    )
    smartbendingrep_79 >> barrier_bend_monitoring_1_dgdvf__alteryxselect_80
    (
        barrier_bend_monitoring_1_dgdvf__alteryxselect_84._out(0)
        >> [portfoliocomposertable_98._in(0), barrierbendingm_74._in(0)]
    )
    smartbendingrep_67 >> barrier_bend_monitoring_1_dgdvf__formula_72_3._in(1)
    (
        barreport_apac__1._out(0)
        >> [barrier_bend_monitoring_1_dgdvf__alteryxselect_84._in(0),
              barrier_bend_monitoring_1_dgdvf__formula_85_0._in(0),
              barrier_bend_monitoring_1_dgdvf__formula_85_0._in(1),
              barrier_bend_monitoring_1_dgdvf__formula_85_0._in(2)]
    )
    barrierbendingm_5 >> barrier_bend_monitoring_1_dgdvf__formula_85_0._in(3)
    smartbendingrep_64 >> barrier_bend_monitoring_1_dgdvf__formula_72_3._in(0)
    textinput_9 >> barrier_bend_monitoring_1_dgdvf__formula_85_0._in(4)
    (
        barrier_bend_monitoring_1_dgdvf__formula_72_3._out(0)
        >> [barrierbendingm_73._in(0), barrier_bend_monitoring_1_dgdvf__formula_85_0._in(5)]
    )
    (
        barrier_bend_monitoring_1_dgdvf__formula_85_0._out(0)
        >> [barrierbendingm_102._in(0), barrier_bend_monitoring_1_dgdvf__alteryxselect_84._in(1),
              barrier_bend_monitoring_1_dgdvf__alteryxselect_84._in(2)]
    )
