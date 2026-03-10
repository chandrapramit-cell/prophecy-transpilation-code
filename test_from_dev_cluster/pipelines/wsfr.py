from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "wsfr", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    table_0 = Process(
        name = "Table_0",
        properties = Dataset(table = Dataset.DBTSource(name = "sf", sourceType = "Seed"), writeOptions = {"writeMode" : "overwrite"}),
        input_ports = None
    )
    wsfr__flattenschema_1 = Process(
        name = "wsfr__FlattenSchema_1",
        properties = ModelTransform(modelName = "wsfr__FlattenSchema_1")
    )
    table_0 >> wsfr__flattenschema_1
