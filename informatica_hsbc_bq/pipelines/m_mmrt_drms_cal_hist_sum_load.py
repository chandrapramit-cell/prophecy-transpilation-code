from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_mmrt_drms_cal_hist_sum_load",
    version = 1,
    auto_layout = False,
    params = Parameters(MP_BUSDATE = "''", username = "''", SCHEMA = "'SCHEMA'", jdbcUrl = "''", password = "''")
)

with Pipeline(args) as pipeline:
    m_mmrt_drms_cal_hist_sum_load__sm_drms_cal_mth_end_hist = Process(
        name = "m_mmrt_drms_cal_hist_sum_load__SM_DRMS_CAL_MTH_END_HIST",
        properties = ModelTransform(modelName = "m_mmrt_drms_cal_hist_sum_load__SM_DRMS_CAL_MTH_END_HIST"),
        input_ports = None
    )

