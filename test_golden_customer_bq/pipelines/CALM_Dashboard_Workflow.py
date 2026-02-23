from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "CALM_Dashboard_Workflow",
    version = 1,
    auto_layout = False,
    params = Parameters(
      password_Database__repor_88 = "''",
      username_Database__loadi_234 = "''",
      username_Database__repor_32 = "''",
      username_Database__repor_9 = "''",
      password_Database__repor_12 = "''",
      jdbcUrl_Database__loadi_234 = "''",
      password_Database__loadi_234 = "''",
      password_Database__loadi_237 = "''",
      username_Database__repor_1 = "''",
      username_Database__repor_31 = "''",
      password_Query_SELECTF___4 = "''",
      password_Database__repor_31 = "''",
      workflow_name = "'CALM_Dashboard_Workflow'",
      username_Query_SELECTF___4 = "''",
      username_Database__loadi_298 = "''",
      username_Database__repor_88 = "''",
      username_Database__repor_118 = "''",
      password_Database__loadi_298 = "''",
      password_Database__repor_9 = "''",
      password_Database__repor_32 = "''",
      password_Database__repor_50 = "''",
      username_Database__repor_49 = "''",
      username_Database__repor_6 = "''",
      username_Database__REPOR_301 = "''",
      username_Database__repor_12 = "''",
      password_Database__repor_1 = "''",
      password_Database__repor_49 = "''",
      username_Database__repor_50 = "''",
      password_Database__repor_11 = "''",
      password_Database__repor_6 = "''",
      username_Database__loadi_236 = "''",
      password_Database__repor_118 = "''",
      jdbcUrl_Database__loadi_298 = "''",
      password_Database__loadi_236 = "''",
      username_Database__loadi_237 = "''",
      username_Database__repor_11 = "''",
      password_Database__REPOR_301 = "''"
    )
)

with Pipeline(args) as pipeline:
    crosstab_115_rename = Process(
        name = "CrossTab_115_rename",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "from pyspark.sql.functions import col\n\nactions_in_use = {\"Avg\"}\nactions_without_prefix = {\"First\", \"Last\", \"Sum\"}\n\ndef rename_column(col_name):\n    last_underscore_index = col_name.rfind(\"_\")\n    if last_underscore_index != -1:\n        first_half = col_name[:last_underscore_index]\n        second_half = col_name[last_underscore_index + 1:]\n\n        if second_half == \"Concat\":\n            return first_half\n        elif len(actions_in_use) == 1 and actions_without_prefix.issubset(actions_in_use) and second_half in actions_without_prefix:\n            return first_half\n        elif second_half in actions_in_use:\n            return f\"{second_half}_{first_half}\"\n        else:\n            return col_name\n    else:\n        return col_name\n\nrenamed_columns = [rename_column(col_name) for col_name in in0.columns]\n\nvalid_columns = [\n    (orig_col, renamed_col)\n    for orig_col, renamed_col in zip(in0.columns, renamed_columns)\n    if not renamed_col.endswith(\"_dummy_\")\n]\n\nfiltered_dataframe = in0.select([col(orig_col) for orig_col, _ in valid_columns])\nvalid_renamed_columns = [renamed_col for _, renamed_col in valid_columns]\n\nout0 = filtered_dataframe.toDF(*valid_renamed_columns)\n"
        ),
        is_custom_output_schema = True
    )
    calm_dashboard_workflow__crosstab_115_1 = Process(
        name = "CALM_Dashboard_Workflow__CrossTab_115_1",
        properties = ModelTransform(modelName = "CALM_Dashboard_Workflow__CrossTab_115_1")
    )
    calm_dashboard_workflow__database__loadi_298 = Process(
        name = "CALM_Dashboard_Workflow__Database__loadi_298",
        properties = ModelTransform(modelName = "CALM_Dashboard_Workflow__Database__loadi_298")
    )
    calm_dashboard_workflow__database__loadi_234 = Process(
        name = "CALM_Dashboard_Workflow__Database__loadi_234",
        properties = ModelTransform(modelName = "CALM_Dashboard_Workflow__Database__loadi_234"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14", "in_15", "in_16", "in_17", "in_18", "in_19", "in_20", "in_21", "in_22", "in_23",
         "in_24", "in_25", "in_26", "in_27", "in_28", "in_29", "in_30", "in_31"]
    )
    crosstab_115_regularactions = Process(
        name = "CrossTab_115_regularActions",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: Dataframe) -> (Dataframe):",
          scriptMethodFooter = "return (out0)",
          script = "\nfrom pyspark.sql.functions import col, collect_list, lit, concat_ws\n\ndf1 = in0.groupBy(col(\"DT\"))\ndf2 = df1.pivot(\"KPI\")\n\nreturn df2.agg(avg(col(\"ANC_PCT\")).alias(\"Avg\"), lit(1).alias(\"_dummy_\"))\n"
        ),
        is_custom_output_schema = True
    )
    crosstab_115_regularactions >> crosstab_115_rename
    crosstab_115_rename >> calm_dashboard_workflow__database__loadi_234._in(28)
    calm_dashboard_workflow__crosstab_115_1 >> crosstab_115_regularactions
    calm_dashboard_workflow__database__loadi_234 >> calm_dashboard_workflow__database__loadi_298
