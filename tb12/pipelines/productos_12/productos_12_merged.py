from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "productos_12_merged", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    productos_12_part1_pipeline = Process(
        name = "productos_12_part1_pipeline",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "productos_12_part1",
          parameters = {}
        )
    )
    productos_12_part2_pipeline = Process(
        name = "productos_12_part2_pipeline",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "productos_12_part2",
          parameters = {}
        )
    )
    productos_12_part1_pipeline >> productos_12_part2_pipeline
