from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118",
    version = 1,
    auto_layout = False,
    params = Parameters(iteration_number = 0)
)

with Pipeline(args) as pipeline:
    databricks_calculation_engine_monthly_master_ec2_version_mom_3118__alteryxselect_852_3118 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__AlteryxSelect_852_3118",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__AlteryxSelect_852_3118"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3118__appendfields_842_3118 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__AppendFields_842_3118",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__AppendFields_842_3118"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3118__filter_844_3118 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_844_3118",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_844_3118")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3118__filter_863_3118_to_filter_870_3118 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_863_3118_to_Filter_870_3118",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_863_3118_to_Filter_870_3118"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3118__filter_875_3118 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_875_3118",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_875_3118")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3118__filter_875_3118_reject = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_875_3118_reject",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_875_3118_reject"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3118__formula_845_3118_1 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Formula_845_3118_1",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Formula_845_3118_1"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3118__recordid_1270_3118 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__RecordID_1270_3118",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__RecordID_1270_3118"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3118__summarize_848_3118 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Summarize_848_3118",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Summarize_848_3118"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3118__table_3118_exit_macro_op = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__table_3118_Exit_macro_op",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__table_3118_Exit_macro_op"
        ),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3118__table_3118_loop_macro_op = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__table_3118_Loop_macro_op",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__table_3118_Loop_macro_op"
        )
    )
    statichistoryfu_854_3118 = Process(
        name = "StaticHistoryFu_854_3118",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "X:\\Engagements\\Gold\\5. Output Data\\Static History Full Pre Macro Input.yxdb",
              "username": "${username_StaticHistoryFu_854_3118}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "StaticHistoryFu_854_3118"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "X:\\Engagements\\Gold\\5. Output Data\\Static History Full Pre Macro Input.yxdb"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118/StaticHistoryFu_854_3118.yml"
          )
        ),
        input_ports = None
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3118__recordid_1270_3118._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3118__table_3118_exit_macro_op._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3118__formula_845_3118_1._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3118__filter_863_3118_to_filter_870_3118._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3118__table_3118_exit_macro_op._in(3),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3118__filter_875_3118._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3118__filter_875_3118_reject._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3118__appendfields_842_3118._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3118__alteryxselect_852_3118._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3118__table_3118_loop_macro_op._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3118__formula_845_3118_1._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3118__table_3118_exit_macro_op._in(4),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3118__filter_863_3118_to_filter_870_3118._in(
                1
              )]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3118__summarize_848_3118._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3118__table_3118_exit_macro_op._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3118__appendfields_842_3118._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3118__filter_844_3118._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3118__filter_863_3118_to_filter_870_3118._in(
                0
              ),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3118__recordid_1270_3118._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3118__alteryxselect_852_3118._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3118__table_3118_exit_macro_op._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3118__filter_844_3118._in(0)]
    )
