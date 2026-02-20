from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Household_Clustering_1_Household_Creation_Priority_2",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_aka_alxaa2_Quer_1 = "''",
      password_aka_alxaa2_Quer_1 = "''",
      workflow_name = "'Household_Clustering_1_Household_Creation_Priority_2'"
    )
)

with Pipeline(args) as pipeline:
    household_clustering_1_household_creation_priority_2__filter_27_reject = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Filter_27_reject",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Filter_27_reject")
    )
    household_clustering_1_household_creation_priority_2__summarize_12 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_12",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_12")
    )
    household_clustering_1_household_creation_priority_2__summarize_19 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_19",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_19")
    )
    household_clustering_1_household_creation_priority_2__unique_4 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Unique_4",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Unique_4")
    )
    household_clustering_1_household_creation_priority_2__cleanse_11 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Cleanse_11",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Cleanse_11")
    )
    household_clustering_1_household_creation_priority_2__summarize_22 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_22",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_22"),
        input_ports = ["in_0", "in_1"]
    )
    household_clustering_1_household_creation_priority_2__summarize_26 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_26",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_26")
    )
    household_clustering_1_household_creation_priority_2__filter_13 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Filter_13",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Filter_13")
    )
    household_clustering_1_household_creation_priority_2__summarize_24 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_24",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_24")
    )
    household_clustering_1_household_creation_priority_2__summarize_21 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_21",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_21"),
        input_ports = ["in_0", "in_1"]
    )
    household_clustering_1_household_creation_priority_2__join_14_left = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Join_14_left",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Join_14_left"),
        input_ports = ["in_0", "in_1"]
    )
    household_clustering_1_household_creation_priority_2__filter_17 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Filter_17",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Filter_17")
    )
    household_clustering_1_household_creation_priority_2__summarize_25 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_25",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_25")
    )
    household_clustering_1_household_creation_priority_2__summarize_20 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_20",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_20")
    )
    household_clustering_1_household_creation_priority_2__filter_27 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Filter_27",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Filter_27")
    )
    household_clustering_1_household_creation_priority_2__recast_cache_cs_2 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__recast_cache_cs_2",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__recast_cache_cs_2")
    )
    household_clustering_1_household_creation_priority_2__address_for_geo_18 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__address_for_geo_18",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__address_for_geo_18")
    )
    (
        household_clustering_1_household_creation_priority_2__unique_4._out(0)
        >> [household_clustering_1_household_creation_priority_2__cleanse_11._in(0),
              household_clustering_1_household_creation_priority_2__summarize_20._in(0),
              household_clustering_1_household_creation_priority_2__summarize_24._in(0),
              household_clustering_1_household_creation_priority_2__recast_cache_cs_2._in(0)]
    )
    (
        household_clustering_1_household_creation_priority_2__cleanse_11._out(0)
        >> [household_clustering_1_household_creation_priority_2__join_14_left._in(0),
              household_clustering_1_household_creation_priority_2__summarize_21._in(0),
              household_clustering_1_household_creation_priority_2__summarize_12._in(0)]
    )
    (
        household_clustering_1_household_creation_priority_2__join_14_left._out(0)
        >> [household_clustering_1_household_creation_priority_2__summarize_22._in(1),
              household_clustering_1_household_creation_priority_2__filter_17._in(0),
              household_clustering_1_household_creation_priority_2__summarize_25._in(0)]
    )
    (
        household_clustering_1_household_creation_priority_2__filter_17._out(0)
        >> [household_clustering_1_household_creation_priority_2__summarize_26._in(0),
              household_clustering_1_household_creation_priority_2__summarize_22._in(0),
              household_clustering_1_household_creation_priority_2__address_for_geo_18._in(0)]
    )
    (
        household_clustering_1_household_creation_priority_2__summarize_12._out(0)
        >> [household_clustering_1_household_creation_priority_2__filter_13._in(0),
              household_clustering_1_household_creation_priority_2__summarize_19._in(0)]
    )
    (
        household_clustering_1_household_creation_priority_2__filter_13._out(0)
        >> [household_clustering_1_household_creation_priority_2__filter_27._in(0),
              household_clustering_1_household_creation_priority_2__filter_27_reject._in(0),
              household_clustering_1_household_creation_priority_2__join_14_left._in(1),
              household_clustering_1_household_creation_priority_2__summarize_21._in(1)]
    )
