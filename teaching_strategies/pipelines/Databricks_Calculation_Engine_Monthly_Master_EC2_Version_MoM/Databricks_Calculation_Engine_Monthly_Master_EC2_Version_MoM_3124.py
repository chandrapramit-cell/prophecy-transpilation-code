from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124",
    version = 1,
    auto_layout = False,
    params = Parameters(iteration_number = 0)
)

with Pipeline(args) as pipeline:
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__alteryxselect_25_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__AlteryxSelect_25_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__AlteryxSelect_25_3124"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_107_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_107_3124",
        properties = ModelTransform(modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_107_3124")
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_107_3124_reject = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_107_3124_reject",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_107_3124_reject"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_126_3124_reject = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_126_3124_reject",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_126_3124_reject"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_135_3124_reject = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_135_3124_reject",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_135_3124_reject"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__formula_145_3124_0 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Formula_145_3124_0",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Formula_145_3124_0"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_112_3124_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_112_3124_inner",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_112_3124_inner"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_121_3124_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_121_3124_inner",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_121_3124_inner"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_150_3124_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_150_3124_inner",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_150_3124_inner"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_152_3124_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_152_3124_inner",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_152_3124_inner"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_153_3124_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_153_3124_inner",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_153_3124_inner"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_158_3124_inner = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_158_3124_inner",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_158_3124_inner"
        ),
        input_ports = ["in_0", "in_1"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_110_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_110_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_110_3124"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_129_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_129_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_129_3124"
        ),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_148_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_148_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_148_3124"
        ),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_114_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_114_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_114_3124"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_116_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_116_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_116_3124"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_118_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_118_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_118_3124"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_123_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_123_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_123_3124"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_156_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_156_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_156_3124"
        ),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_161_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_161_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_161_3124"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_163_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_163_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_163_3124"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_171_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_171_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_171_3124"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_173_3124 = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_173_3124",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_173_3124"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__table_3124_exit_macro_op = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__table_3124_Exit_macro_op",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__table_3124_Exit_macro_op"
        )
    )
    databricks_calculation_engine_monthly_master_ec2_version_mom_3124__table_3124_loop_macro_op = Process(
        name = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__table_3124_Loop_macro_op",
        properties = ModelTransform(
          modelName = "Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__table_3124_Loop_macro_op"
        ),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_156_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_161_3124._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_173_3124._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__table_3124_exit_macro_op._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__formula_145_3124_0._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__alteryxselect_25_3124._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_150_3124_inner._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_107_3124_reject._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_129_3124._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_114_3124._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_171_3124._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_129_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_148_3124._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_126_3124_reject._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_114_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_129_3124._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_116_3124._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_110_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_107_3124._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_107_3124_reject._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_171_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_156_3124._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_129_3124._in(4)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_118_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_129_3124._in(3),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_112_3124_inner._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_163_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__table_3124_loop_macro_op._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_158_3124_inner._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_126_3124_reject._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_148_3124._in(3),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_123_3124._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_123_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_148_3124._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_121_3124_inner._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__alteryxselect_25_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_153_3124_inner._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_156_3124._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_152_3124_inner._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_148_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_153_3124_inner._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_150_3124_inner._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__formula_145_3124_0._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_135_3124_reject._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_153_3124_inner._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__table_3124_loop_macro_op._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_163_3124._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_107_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_156_3124._in(3),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_129_3124._in(5),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_118_3124._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__filter_135_3124_reject._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_156_3124._in(4),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__formula_145_3124_0._in(1)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_116_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_148_3124._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_121_3124_inner._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__recordid_129_3124._in(2),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_112_3124_inner._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_161_3124._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__table_3124_loop_macro_op._in(0),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_158_3124_inner._in(0)]
    )
    (
        databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_150_3124_inner._out(0)
        >> [databricks_calculation_engine_monthly_master_ec2_version_mom_3124__summarize_156_3124._in(1),
              databricks_calculation_engine_monthly_master_ec2_version_mom_3124__join_152_3124_inner._in(1)]
    )
