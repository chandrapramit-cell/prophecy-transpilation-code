from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_mmrt_credit_card_plan_cal_hist_sum_load",
    version = 1,
    auto_layout = False,
    params = Parameters(MP_BUSDATE = "''", username = "''", SCHEMA = "'SCHEMA'", jdbcUrl = "''", password = "''")
)

with Pipeline(args) as pipeline:
    m_mmrt_credit_card_plan_cal_hist_sum_load__sm_credit_card_plan_cal_mth_his = Process(
        name = "m_mmrt_credit_card_plan_cal_hist_sum_load__SM_CREDIT_CARD_PLAN_CAL_MTH_HIS",
        properties = ModelTransform(modelName = "m_mmrt_credit_card_plan_cal_hist_sum_load__SM_CREDIT_CARD_PLAN_CAL_MTH_HIS"),
        input_ports = None
    )

