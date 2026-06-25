from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "float_cast_squaring", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    float_cast_squaring__cast_and_square = Process(
        name = "float_cast_squaring__cast_and_square",
        properties = ModelTransform(modelName = "float_cast_squaring__cast_and_square"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    float_sample_data = Process(
        name = "float_sample_data",
        properties = Dataset(table = Dataset.DBTSource(name = "float_sample_data", sourceType = "Seed")),
        input_ports = None
    )
    (
        float_sample_data._out(0)
        >> [float_cast_squaring__cast_and_square._in(0), float_cast_squaring__cast_and_square._in(1),
              float_cast_squaring__cast_and_square._in(2)]
    )
