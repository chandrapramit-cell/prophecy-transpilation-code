from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "sample_recordid",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'sample_recordid'")
)

with Pipeline(args) as pipeline:
    table_1 = Process(
        name = "Table_1",
        properties = Dataset(
          table = Dataset.DBTSource(name = "sffgfbweg", sourceType = "Seed"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None
    )
    sample_recordid__recordid_1 = Process(
        name = "sample_recordid__RecordID_1",
        properties = ModelTransform(modelName = "sample_recordid__RecordID_1")
    )
    table_1 >> sample_recordid__recordid_1
