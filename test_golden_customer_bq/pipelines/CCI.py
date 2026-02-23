from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "CCI",
    version = 1,
    auto_layout = False,
    params = Parameters(
      password_dcm_phpdatabase_49 = "''",
      username_dcm_phpdatabase_72 = "''",
      username_dcm_phpdatabase_47 = "''",
      username_dcm_phpdatabase_3 = "''",
      password_dcm_phpdatabase_4 = "''",
      username_dcm_phpdatabase_4 = "''",
      workflow_name = "'CCI'",
      password_CCI_Scores_yxdb_81 = "''",
      password_dcm_phpdatabase_72 = "''",
      username_dcm_phpdatabase_49 = "''",
      password_dcm_phpdatabase_47 = "''",
      username_CCI_Scores_yxdb_81 = "''",
      jdbcUrl_CCI_Scores_yxdb_81 = "''",
      password_dcm_phpdatabase_3 = "''"
    )
)

with Pipeline(args) as pipeline:
    cci__unique_78 = Process(
        name = "CCI__Unique_78",
        properties = ModelTransform(modelName = "CCI__Unique_78"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    cci__summarize_62 = Process(name = "CCI__Summarize_62", properties = ModelTransform(modelName = "CCI__Summarize_62"))
    cci__formula_73_0 = Process(name = "CCI__Formula_73_0", properties = ModelTransform(modelName = "CCI__Formula_73_0"))
    cci__summarize_71 = Process(name = "CCI__Summarize_71", properties = ModelTransform(modelName = "CCI__Summarize_71"))
    cci__cci_break_out_c_80 = Process(
        name = "CCI__CCI_Break_Out_c_80",
        properties = ModelTransform(modelName = "CCI__CCI_Break_Out_c_80")
    )
    cci__alteryxselect_69 = Process(
        name = "CCI__AlteryxSelect_69",
        properties = ModelTransform(modelName = "CCI__AlteryxSelect_69"),
        input_ports = ["in_0", "in_1"]
    )
    cci__cci_scores_yxdb_81 = Process(
        name = "CCI__CCI_Scores_yxdb_81",
        properties = ModelTransform(modelName = "CCI__CCI_Scores_yxdb_81"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    cci__join_74_inner = Process(
        name = "CCI__Join_74_inner",
        properties = ModelTransform(modelName = "CCI__Join_74_inner"),
        input_ports = ["in_0", "in_1"]
    )
    cci__alteryxselect_69._out(0) >> [cci__unique_78._in(3), cci__summarize_62._in(0)]
    cci__formula_73_0._out(0) >> [cci__cci_scores_yxdb_81._in(5), cci__join_74_inner._in(1)]
    (
        cci__summarize_71._out(0)
        >> [cci__cci_scores_yxdb_81._in(1), cci__cci_scores_yxdb_81._in(3), cci__join_74_inner._in(0)]
    )
    cci__summarize_62._out(0) >> [cci__cci_scores_yxdb_81._in(0), cci__unique_78._in(1)]
    cci__unique_78._out(0) >> [cci__summarize_71._in(0), cci__cci_break_out_c_80._in(0)]
