from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "psp", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    ii_0 = Process(
        name = "ii_0",
        properties = CallStoredProc(
          storedProcedureIdentifier = "prophecy-databricks-qa.avpreetModels.ii",
          parameters = {"abc" : "CAST(id as INT64)"},
          passThroughColumns = [{"alias" : "id", "expression" : {"expression" : "id"}},
           {"alias" : "col", "expression" : {"expression" : "col"}},
           {"alias" : "val", "expression" : {"expression" : "val"}}]
        )
    )
    sf = Process(
        name = "sf",
        properties = Dataset(writeOptions = {"writeMode" : "overwrite"}, table = Dataset.DBTSource(name = "sf", sourceType = "Seed")),
        input_ports = None
    )
    sf >> ii_0
