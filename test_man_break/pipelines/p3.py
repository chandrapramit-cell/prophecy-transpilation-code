from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p3", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    pipeline_0 = Process(
        name = "Pipeline_0",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "p1",
          parameterSet = ""
        )
    )
    pipeline_1 = Process(
        name = "Pipeline_1",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "p2",
          parameterSet = ""
        )
    )
    pipeline_0 >> pipeline_1
