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
          ),
          compression = DatabricksVolumeSource.Compression(kind = "uncompressed")
        ),
        input_ports = None
    )
    barrierbendingm_102 = Process(
        name = "BarrierBendingM_102",
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
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Output files BB\\Barrier Bending  Monitoring -APAC.xlsx"
          )
        )
    )
    barrierbendingm_5 = Process(
        name = "BarrierBendingM_5",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Input files BB\\Barrier Bending  Monitoring -APAC.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(sheetName = "Sheet24"),
          compression = DatabricksVolumeSource.Compression(kind = "uncompressed")
        ),
        input_ports = None,
        comment = "Loads APAC Barrier Bending monitoring Excel to provide trade-level barrier metrics, review dates, desk assignments, and acceptance/comments for downstream risk and operational monitoring."
    )
    barrierbendingm_73 = Process(
        name = "BarrierBendingM_73",
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
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Output files BB\\Barrier Bending  Monitoring -APAC.xlsx"
          )
        )
    )
    barrierbendingm_74 = Process(
        name = "BarrierBendingM_74",
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
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Output files BB\\Barrier Bending  Monitoring -APAC.xlsx"
          )
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
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Input files BB\\SmartBendingReport*.xls"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Barrier_Bend_Monitoring_1_dgdvf/SmartBendingRep_64.yml",
            sheetName = "ASSET_FILTERING",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    smartbendingrep_67 = Process(
        name = "SmartBendingRep_67",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Input files BB\\SmartBendingReport*.xls"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            FilterValue = "'ABC'",
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            FilterOption = "IsIn",
            schema = "external_sources/Barrier_Bend_Monitoring_1_dgdvf/SmartBendingRep_67.yml",
            sheetName = "PUT2FWD_NOKO",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          ),
          compression = DatabricksVolumeSource.Compression(kind = "uncompressed")
        ),
        input_ports = None,
        comment = "Loads SmartBendingReport Excel files to capture instrument valuations, knock-out levels, positions and portfolio fair\u2011value impacts for APAC equity risk reporting."
    )
    smartbendingrep_79 = Process(
        name = "SmartBendingRep_79",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "B:\\Equity\\Controllers\\Saurabh\\APAC - Local\\Altryx\\Barrier Bend Automation\\Input files BB\\SmartBendingReport*.xls"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(sheetName = "Sheet45"),
          compression = DatabricksVolumeSource.Compression(kind = "uncompressed")
        ),
        input_ports = None,
        comment = "Loads SmartBendingReport Excel files to supply bending metrics for APAC equity controllers' automated reporting and processing."
    )
    textinput_9 = Process(
        name = "TextInput_9",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_Barrier_Bend_Monitoring_1__9", sourceType = "Seed")
        ),
        input_ports = None,
        comment = "Overwrites the Barrier Bend Monitoring seed dataset to refresh the monitoring baseline."
    )
    smartbendingrep_79 >> barrier_bend_monitoring_1_dgdvf__alteryxselect_80
    (
        barrier_bend_monitoring_1_dgdvf__formula_72_3._out(0)
        >> [barrierbendingm_73._in(0), barrier_bend_monitoring_1_dgdvf__formula_85_0._in(5)]
    )
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
        barrier_bend_monitoring_1_dgdvf__formula_85_0._out(0)
        >> [barrierbendingm_102._in(0), barrier_bend_monitoring_1_dgdvf__alteryxselect_84._in(1),
              barrier_bend_monitoring_1_dgdvf__alteryxselect_84._in(2)]
    )
