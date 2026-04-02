from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    p1__undefined_schema_xml = Process(
        name = "p1__undefined_schema_xml",
        properties = ModelTransform(modelName = "p1__undefined_schema_xml")
    )
    undefined_output_xml = Process(
        name = "undefined_output_xml",
        properties = Dataset(
          table = Dataset.DBTSource(name = "sr2", sourceType = "Seed"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None,
        comment = "Overwrites the seed data for the sr2 dataset to refresh the source."
    )
    undefined_output_xml >> p1__undefined_schema_xml
