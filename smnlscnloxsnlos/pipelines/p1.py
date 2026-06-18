from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    orchestrationsource_0 = Process(
        name = "OrchestrationSource_0",
        properties = SFTPSource(
          format = SFTPSource.CsvReadFormat(),
          compression = SFTPSource.Compression(kind = "uncompressed"),
          properties = SFTPSource.SFTPSourceInternal(),
          connector = {"kind" : "sftp", "type" : "connector", "properties" : {}}
        ),
        input_ports = None
    )

