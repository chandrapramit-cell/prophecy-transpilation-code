from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "productos_merged", version = 1)

with Pipeline(args) as pipeline:
    productos_part1_pipeline = Process(
        name = "productos_part1_pipeline",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "productos_part1",
          parameters = {}
        )
    )
    productos_part2_pipeline = Process(
        name = "productos_part2_pipeline",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "productos_part2",
          parameters = {}
        )
    )
    productos_part1_pipeline >> productos_part2_pipeline
