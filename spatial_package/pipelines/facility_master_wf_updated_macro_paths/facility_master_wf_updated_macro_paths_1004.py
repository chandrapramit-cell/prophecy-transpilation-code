from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "facility_master_wf_updated_macro_paths_1004",
    version = 1,
    auto_layout = False,
    params = Parameters(
      Control_Parameter = "'(1)'",
      variable26_Expression = Expr("[GroupID] IN {{ var('Control_Parameter') }}"),
      workflow_name = "'facility_master_wf_updated_macro_paths'",
      Question__Macro_Output_32 = "''",
      Question__ControlParam__Control_Parameter_29 = "''",
      Question__Macro_Input_30 = "''"
    )
)

with Pipeline(args) as pipeline:
    makegroup_40_1004 = Process(
        name = "MakeGroup_40_1004",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: Dataframe) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "\n\nfrom pyspark.sql import DataFrame, SparkSession\n\ndef find(key: str, group_map: dict) -> str:\n    if group_map[key] != key:\n        group_map[key] = find(group_map[key], group_map)\n    return group_map[key]\n\ndef find_connected_components_with_hashmap(df: DataFrame) -> DataFrame:\n    group_map = {}\n\n    for row in df.collect():\n        key_a = row[0]  # Accessing the first column (key_A)\n        key_b = row[1]  # Accessing the second column (key_B)\n\n        if key_a not in group_map:\n            group_map[key_a] = key_a\n        if key_b not in group_map:\n            group_map[key_b] = key_b\n\n        group_a = find(key_a, group_map)\n        group_b = find(key_b, group_map)\n\n        if group_a != group_b:\n            if group_a < group_b:\n                group_map[group_b] = group_a\n            else:\n                group_map[group_a] = group_b\n\n    final_map = {key: find(key, group_map) for key in group_map.keys()}\n    final_df = spark.createDataFrame(final_map.items(), [\"variableKey\", \"variableGroup\"])\n\n    return final_df\n\nout0 = find_connected_components_with_hashmap(in0.select(\"organization_name\", \"organization_name2\"))\n\n"
        ),
        is_custom_output_schema = True
    )
    facility_master_wf_updated_macro_paths_1004__filter_26_1004 = Process(
        name = "facility_master_wf_updated_macro_paths_1004__Filter_26_1004",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths_1004__Filter_26_1004")
    )
    facility_master_wf_updated_macro_paths_1004__fuzzymatch_41_1004_reformat = Process(
        name = "facility_master_wf_updated_macro_paths_1004__FuzzyMatch_41_1004_reformat",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths_1004__FuzzyMatch_41_1004_reformat")
    )
    facility_master_wf_updated_macro_paths_1004__table_1004_output6_macro_op = Process(
        name = "facility_master_wf_updated_macro_paths_1004__table_1004_Output6_macro_op",
        properties = ModelTransform(modelName = "facility_master_wf_updated_macro_paths_1004__table_1004_Output6_macro_op"),
        input_ports = ["in_0", "in_1"]
    )
    makegroup_40_1004 >> facility_master_wf_updated_macro_paths_1004__table_1004_output6_macro_op._in(1)
    facility_master_wf_updated_macro_paths_1004__fuzzymatch_41_1004_reformat >> makegroup_40_1004
    (
        facility_master_wf_updated_macro_paths_1004__filter_26_1004._out(0)
        >> [facility_master_wf_updated_macro_paths_1004__table_1004_output6_macro_op._in(0),
              facility_master_wf_updated_macro_paths_1004__fuzzymatch_41_1004_reformat._in(0)]
    )
