from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "seed_join_pipeline", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    seed_catalogues = Process(
        name = "seed_catalogues",
        properties = Dataset(table = Dataset.DBTSource(name = "seed_catalogues", sourceType = "Seed")),
        input_ports = None
    )
    seed_join_pipeline__reserved_keywords_output = Process(
        name = "seed_join_pipeline__reserved_keywords_output",
        properties = ModelTransform(modelName = "seed_join_pipeline__reserved_keywords_output")
    )
    seed_join_pipeline__seed_table_price_descending = Process(
        name = "seed_join_pipeline__seed_table_price_descending",
        properties = ModelTransform(modelName = "seed_join_pipeline__seed_table_price_descending"),
        input_ports = ["in_0", "in_1"]
    )
    seed_reserved_keywords = Process(
        name = "seed_reserved_keywords",
        properties = Dataset(table = Dataset.DBTSource(name = "seed_reserved_keywords", sourceType = "Seed")),
        input_ports = None
    )
    seed_table_prices = Process(
        name = "seed_table_prices",
        properties = Dataset(table = Dataset.DBTSource(name = "seed_table_prices", sourceType = "Seed")),
        input_ports = None,
        comment = "Imports seed price data into the workflow to initialize pricing analytics."
    )
    seed_table_prices >> seed_join_pipeline__seed_table_price_descending._in(0)
    seed_catalogues >> seed_join_pipeline__seed_table_price_descending._in(1)
    seed_reserved_keywords >> seed_join_pipeline__reserved_keywords_output
