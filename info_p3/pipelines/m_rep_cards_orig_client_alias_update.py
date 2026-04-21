from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_client_alias_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    exp_client_alias_vars = Process(
        name = "EXP_CLIENT_ALIAS_VARS",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out",
          script = "\nbusiness_date_string = \"\"\nrows = []\nfor row in in0.toLocalIterator():\n  data = element.asDict(recursive=True)\n  business_date_string = cant_parse(\"business_date_string\")\n  data[\"business_date_string\"] = business_date_string\n  rows += [Row(**data)]\nout0 = spark.createDataFrame(rows)\n"
        ),
        is_custom_output_schema = True
    )
    feed_number_generator1 = Process(
        name = "feed_number_generator1",
        properties = CallStoredProc(
          storedProcedureIdentifier = "generate_feed_id",
          parameters = {"IN_SOURCE_SYSTEM_CODE" : "dummy"},
          passThroughColumns = []
        ),
        input_ports = None
    )
    m_rep_cards_orig_client_alias_update__chooses = Process(
        name = "m_rep_cards_orig_client_alias_update__CHOOSES",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__CHOOSES")
    )
    m_rep_cards_orig_client_alias_update__client_alias1 = Process(
        name = "m_rep_cards_orig_client_alias_update__CLIENT_ALIAS1",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__CLIENT_ALIAS1")
    )
    m_rep_cards_orig_client_alias_update__exp_client_alias_lookup_6 = Process(
        name = "m_rep_cards_orig_client_alias_update__EXP_CLIENT_ALIAS_LOOKUP_6",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__EXP_CLIENT_ALIAS_LOOKUP_6")
    )
    m_rep_cards_orig_client_alias_update__resides = Process(
        name = "m_rep_cards_orig_client_alias_update__RESIDES",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__RESIDES")
    )
    m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias = Process(
        name = "m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS"),
        input_ports = None
    )
    (
        m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias._out(0)
        >> [m_rep_cards_orig_client_alias_update__chooses._in(0),
              m_rep_cards_orig_client_alias_update__client_alias1._in(0),
              m_rep_cards_orig_client_alias_update__resides._in(0)]
    )
    m_rep_cards_orig_client_alias_update__exp_client_alias_lookup_6 >> exp_client_alias_vars
