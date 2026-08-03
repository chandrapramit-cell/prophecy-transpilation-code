from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "double_formats_pipeline", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    double_formats_pipeline__reformat_doubles = Process(
        name = "double_formats_pipeline__reformat_doubles",
        properties = ModelTransform(modelName = "double_formats_pipeline__reformat_doubles")
    )
    double_formats_seed = Process(
        name = "double_formats_seed",
        properties = Dataset(table = Dataset.DBTSource(name = "double_formats", sourceType = "Seed")),
        input_ports = None,
        comment = "Provides a seeded double_formats table as a stable reference for downstream transformations, validations, and reporting."
    )
    double_formats_seed >> double_formats_pipeline__reformat_doubles
