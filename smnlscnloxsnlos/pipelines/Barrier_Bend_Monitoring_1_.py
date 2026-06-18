from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Barrier_Bend_Monitoring_1_",
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
      workflow_name = "'Barrier_Bend_Monitoring_1_'"
    )
)

with Pipeline(args) as pipeline:
    barrier_bend_monitoring_1___alteryxselect_80 = Process(
        name = "Barrier_Bend_Monitoring_1___AlteryxSelect_80",
        properties = ModelTransform(modelName = "Barrier_Bend_Monitoring_1___AlteryxSelect_80")
    )
    barrier_bend_monitoring_1___barrierbendingm_102 = Process(
        name = "Barrier_Bend_Monitoring_1___BarrierBendingM_102",
        properties = ModelTransform(modelName = "Barrier_Bend_Monitoring_1___BarrierBendingM_102")
    )
    barrier_bend_monitoring_1___barrierbendingm_73 = Process(
        name = "Barrier_Bend_Monitoring_1___BarrierBendingM_73",
        properties = ModelTransform(modelName = "Barrier_Bend_Monitoring_1___BarrierBendingM_73")
    )
    barrier_bend_monitoring_1___barrierbendingm_74 = Process(
        name = "Barrier_Bend_Monitoring_1___BarrierBendingM_74",
        properties = ModelTransform(modelName = "Barrier_Bend_Monitoring_1___BarrierBendingM_74"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    barrier_bend_monitoring_1___formula_72_3 = Process(
        name = "Barrier_Bend_Monitoring_1___Formula_72_3",
        properties = ModelTransform(modelName = "Barrier_Bend_Monitoring_1___Formula_72_3"),
        input_ports = ["in_0", "in_1"]
    )
    barrier_bend_monitoring_1___formula_85_0 = Process(
        name = "Barrier_Bend_Monitoring_1___Formula_85_0",
        properties = ModelTransform(modelName = "Barrier_Bend_Monitoring_1___Formula_85_0"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    portfoliocomposertable_98 = Process(name = "PortfolioComposerTable_98", properties = Visualize(), output_ports = None)
    textinput_9 = Process(
        name = "TextInput_9",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_Barrier_Bend_Monitoring_1__9", sourceType = "Seed")
        ),
        input_ports = None,
        comment = "Overwrites the Barrier Bend Monitoring seed dataset to refresh the monitoring baseline."
    )
    barrier_bend_monitoring_1___barrierbendingm_74 >> portfoliocomposertable_98
    textinput_9 >> barrier_bend_monitoring_1___formula_85_0._in(4)
    (
        barrier_bend_monitoring_1___formula_85_0._out(0)
        >> [barrier_bend_monitoring_1___barrierbendingm_74._in(1),
              barrier_bend_monitoring_1___barrierbendingm_74._in(2),
              barrier_bend_monitoring_1___barrierbendingm_102._in(0)]
    )
    (
        barrier_bend_monitoring_1___formula_72_3._out(0)
        >> [barrier_bend_monitoring_1___formula_85_0._in(5), barrier_bend_monitoring_1___barrierbendingm_73._in(0)]
    )
