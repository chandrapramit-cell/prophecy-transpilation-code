from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "keyword_workflow",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'keyword_workflow'")
)

with Pipeline(args) as pipeline:
    textinput_57 = Process(
        name = "TextInput_57",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_57", sourceType = "Seed")
        ),
        input_ports = None
    )
    keyword_workflow__alteryxselect_43 = Process(
        name = "keyword_workflow__AlteryxSelect_43",
        properties = ModelTransform(modelName = "keyword_workflow__AlteryxSelect_43")
    )
    textinput_57 >> keyword_workflow__alteryxselect_43
