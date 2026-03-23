from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1)

with Pipeline(args) as pipeline:
    orchestrationsource_1 = Process(
        name = "OrchestrationSource_1",
        properties = GCSSource(
          format = GCSSource.CsvReadFormat(),
          compression = GCSSource.Compression(kind = "uncompressed"),
          properties = GCSSource.GCSSourceInternal(),
          connector = {"kind" : "GCS", "properties" : {}, "type" : "connector"}
        ),
        input_ports = None
    )
    p1__fuzzymatch_0 = Process(name = "p1__FuzzyMatch_0", properties = ModelTransform(modelName = "p1__FuzzyMatch_0"))
    orchestrationsource_1 >> p1__fuzzymatch_0
