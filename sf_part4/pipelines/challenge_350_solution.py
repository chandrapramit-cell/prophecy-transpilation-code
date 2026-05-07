from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "challenge_350_solution",
    version = 1,
    auto_layout = False,
    params = Parameters(WORKFLOW_NAME = "'challenge_350_solution'")
)

with Pipeline(args) as pipeline:
    textinput_58 = Process(
        name = "TextInput_58",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_58", sourceType = "Seed")
        ),
        input_ports = None
    )
    challenge_350_solution__portfoliocomposerrender_49 = Process(
        name = "challenge_350_solution__PortfolioComposerRender_49",
        properties = ModelTransform(modelName = "challenge_350_solution__PortfolioComposerRender_49")
    )
    textinput_58 >> challenge_350_solution__portfoliocomposerrender_49
