from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1__76",
    version = 1,
    auto_layout = False,
    params = Parameters(
      CSV_load_path = "'..\\Inputs\\Credits_EM\\GEM_Credit_Trades_20210801.csv'",
      variable1_File = Expr("{{ var('CSV_load_path') }}"),
      workflow_name = "'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_'",
      Question__ControlParam__Control_Parameter_5 = "''"
    )
)

with Pipeline(args) as pipeline:
    cem_securities_pruval_6m_trade_volume_upload_v_1_0_1__76__table_76_output4_macro_op = Process(
        name = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1__76__table_76_Output4_macro_op",
        properties = ModelTransform(
          modelName = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1__76__table_76_Output4_macro_op"
        )
    )

