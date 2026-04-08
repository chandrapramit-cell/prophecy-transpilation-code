from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p2", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    script_0 = Process(
        name = "Script_0",
        properties = Script(ports = None, scriptMethodHeader = "def Script():", scriptMethodFooter = "return", script = "")
    )

