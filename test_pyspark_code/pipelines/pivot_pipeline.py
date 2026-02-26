from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "pivot_pipeline", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    pivot_script = Process(
        name = "pivot_script",
        properties = Script(
          script = "from pyspark.sql.functions import col, collect_list, lit, concat_ws, sum\n\ndf1 = in0.groupBy(col(\"ItemNumber\"))\ndf2 = df1.pivot(\"LocationCode\")\n\nreturn df2.agg(sum(col(\"Sum_AvailableQuantity\")).alias(\"Sum\"), lit(1).alias(\"_dummy_\"))",
          scriptMethodFooter = "return out0",
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:"
        )
    )
    undetermined_format_missing_schema = Process(
        name = "undetermined_format_missing_schema",
        properties = Dataset(
          table = Dataset.DBTSource(name = "wsfr", sourceType = "Seed"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None
    )
    undetermined_format_missing_schema >> pivot_script
