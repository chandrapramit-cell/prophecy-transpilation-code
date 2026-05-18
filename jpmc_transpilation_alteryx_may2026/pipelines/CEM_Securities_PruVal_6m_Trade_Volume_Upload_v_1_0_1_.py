from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_",
    version = 1,
    auto_layout = False,
    params = Parameters(
      Select_the_LOB_ = "''",
      variable18_Disabled_value = "CASE WHEN (Select_the_LOB_ = 'Rates') THEN 'False' ELSE 'True' END",
      variable46_Disabled_value = "CASE WHEN (Select_the_LOB_ = 'Credits DM') THEN 'False' ELSE 'True' END",
      variable74_Disabled_value = "CASE WHEN (Select_the_LOB_ = 'Credits EM') THEN 'False' ELSE 'True' END",
      variable143_Disabled_value = "CASE WHEN (Select_the_LOB_ = 'CEM') THEN 'False' ELSE 'True' END",
      workflow_name = "'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_'",
      Question__Drop_Down_172 = "''"
    )
)

with Pipeline(args) as pipeline:
    pass

