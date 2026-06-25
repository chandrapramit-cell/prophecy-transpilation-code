from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p3", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    p3__sanitized_name = Process(name = "p3__sanitized_name", properties = ModelTransform(modelName = "p3__sanitized_name"))
    unknown_format_requires_schema_validation = Process(
        name = "unknown_format_requires_schema_validation",
        properties = Dataset(table = Dataset.DBTSource(name = "s2", sourceType = "Seed"), writeOptions = {"writeMode" : "overwrite"}),
        input_ports = None,
        comment = "Overwrites the seed table 's2' to refresh baseline data used by downstream reports and processes."
    )
    unknown_format_requires_schema_validation >> p3__sanitized_name
