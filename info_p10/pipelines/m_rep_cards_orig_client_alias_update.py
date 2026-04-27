from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_client_alias_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    dbattribute = Process(
        name = "DBATTRIBUTE",
        properties = Dataset(table = Dataset.DBTSource(name = "sf", sourceType = "Seed"), writeOptions = {"writeMode" : "overwrite"}),
        input_ports = None
    )
    exp_client_alias_vars = Process(
        name = "EXP_CLIENT_ALIAS_VARS",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\ncount1 = 0\nlookup_string = \"\"\nbusiness_date_string__find_business_date_lkp_1 = 0\nbusiness_date_string = \"\"\nbusinessdate = 0\nfeed_update_nbr__feed_number_generator1_sp_2 = 0\nfeed_update_nbr = 0\nrows = []\nfor row in in0.toLocalIterator():\n  data = element.asDict(recursive=True)\n  count1 = count1 + 1\n  lookup_string = \"businessdate\"\n  business_date_string__find_business_date_lkp_1 = LOOKUP_VARIABLE_3\n  business_date_string = business_date_string__find_business_date_lkp_1 if (count1 == 1) else business_date_string\n  businessdate = call_spark_fcn(\"force_error\", \"No Business Date found on dbattribute\") if (bool(isnull(business_date_string))) else to_timestamp(call_spark_fcn(\"string_substring\", business_date_string, 1, 8), \"yyyyMMdd\")\n  feed_update_nbr__feed_number_generator1_sp_2 = feed_number_generator1(\"U\", 360, business_date_string, call_spark_fcn(\"string_substring\", business_date_string, 1, 8))\n  feed_update_nbr = feed_update_nbr__feed_number_generator1_sp_2 if (count1 == 1) else feed_update_nbr\n  data[\"count1\"] = count1\n  data[\"lookup_string\"] = lookup_string\n  data[\"business_date_string__find_business_date_lkp_1\"] = business_date_string__find_business_date_lkp_1\n  data[\"business_date_string\"] = business_date_string\n  data[\"businessdate\"] = businessdate\n  data[\"feed_update_nbr__feed_number_generator1_sp_2\"] = feed_update_nbr__feed_number_generator1_sp_2\n  data[\"feed_update_nbr\"] = feed_update_nbr\n  rows += [Row(**data)]\nout0 = spark.createDataFrame(rows)\n"
        ),
        is_custom_output_schema = True
    )
    storedprocedure_1 = Process(
        name = "StoredProcedure_1",
        properties = CallStoredProc(
          storedProcedureIdentifier = "SP1",
          parameters = {"ABC" : "NAME AS ABC", "CD" : "CURRVALUE AS CD"},
          passThroughColumns = [{"alias" : "EF2", "expression" : {"expression" : "ISNULL(EF)"}}]
        ),
        output_ports = None
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
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS")
    )
    (
        dbattribute._out(0)
        >> [storedprocedure_1._in(0), m_rep_cards_orig_client_alias_update__exp_client_alias_lookup_6._in(0)]
    )
    exp_client_alias_vars >> m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias
    (
        m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias._out(0)
        >> [m_rep_cards_orig_client_alias_update__chooses._in(0),
              m_rep_cards_orig_client_alias_update__resides._in(0),
              m_rep_cards_orig_client_alias_update__client_alias1._in(0)]
    )
    m_rep_cards_orig_client_alias_update__exp_client_alias_lookup_6 >> exp_client_alias_vars
