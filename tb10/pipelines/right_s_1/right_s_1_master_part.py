from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "right_s_1_master_part", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    right_s_1_part1_master_part = Process(
        name = "right_s_1_part1_master_part",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "right_s_1_part1",
          parameters = {}
        )
    )
    right_s_1_part2_master_part = Process(
        name = "right_s_1_part2_master_part",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "right_s_1_part2",
          parameters = {}
        )
    )
    right_s_1_part1_master_part >> right_s_1_part2_master_part
