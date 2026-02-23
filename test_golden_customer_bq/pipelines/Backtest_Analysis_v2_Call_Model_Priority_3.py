from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Backtest_Analysis_v2_Call_Model_Priority_3",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'Backtest_Analysis_v2_Call_Model_Priority_3'")
)

with Pipeline(args) as pipeline:
    backtest_analysis_v2_call_model_priority_3__unique_34 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Unique_34",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Unique_34"),
        input_ports = ["in_0", "in_1"]
    )
    backtest_analysis_v2_call_model_priority_3__formula_93_1 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Formula_93_1",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Formula_93_1")
    )
    backtest_analysis_v2_call_model_priority_3__sort_223 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_223",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_223")
    )
    textinput_19 = Process(
        name = "TextInput_19",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_19", sourceType = "Seed")
        ),
        input_ports = None
    )
    backtest_analysis_v2_call_model_priority_3__sort_100 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_100",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_100")
    )
    backtest_analysis_v2_call_model_priority_3__sort_110 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_110",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_110")
    )
    backtest_analysis_v2_call_model_priority_3__sort_15 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_15",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_15")
    )
    backtest_analysis_v2_call_model_priority_3__sort_11 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_11",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_11")
    )
    backtest_analysis_v2_call_model_priority_3__sort_104 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_104",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_104")
    )
    backtest_analysis_v2_call_model_priority_3__sort_9 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_9",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_9")
    )
    backtest_analysis_v2_call_model_priority_3__alteryxselect_50 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__AlteryxSelect_50",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__AlteryxSelect_50")
    )
    backtest_analysis_v2_call_model_priority_3__summarize_61 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_61",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_61")
    )
    backtest_analysis_v2_call_model_priority_3__sort_13 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_13",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_13")
    )
    backtest_analysis_v2_call_model_priority_3__sort_99 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_99",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_99")
    )
    backtest_analysis_v2_call_model_priority_3__summarize_16 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_16",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_16")
    )
    backtest_analysis_v2_call_model_priority_3__numberofcallsto_231 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__numberofcallsto_231",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__numberofcallsto_231")
    )
    backtest_analysis_v2_call_model_priority_3__sort_5 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_5",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_5")
    )
    backtest_analysis_v2_call_model_priority_3__join_3_inner = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Join_3_inner",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Join_3_inner"),
        input_ports = ["in_0", "in_1"]
    )
    backtest_analysis_v2_call_model_priority_3__join_112_inner = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Join_112_inner",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Join_112_inner"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    backtest_analysis_v2_call_model_priority_3__sort_263 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_263",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_263")
    )
    backtest_analysis_v2_call_model_priority_3__summarize_54 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_54",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_54")
    )
    backtest_analysis_v2_call_model_priority_3__sort_259 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_259",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_259")
    )
    backtest_analysis_v2_call_model_priority_3__summarize_36 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_36",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_36")
    )
    backtest_analysis_v2_call_model_priority_3__join_57_inner = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Join_57_inner",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Join_57_inner"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    backtest_analysis_v2_call_model_priority_3__summarize_17 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_17",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_17")
    )
    backtest_analysis_v2_call_model_priority_3__summarize_315 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_315",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_315")
    )
    backtest_analysis_v2_call_model_priority_3__filter_2 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Filter_2",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Filter_2")
    )
    backtest_analysis_v2_call_model_priority_3__filter_258 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Filter_258",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Filter_258")
    )
    backtest_analysis_v2_call_model_priority_3__sort_7 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_7",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_7")
    )
    backtest_analysis_v2_call_model_priority_3__summarize_52 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_52",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Summarize_52")
    )
    backtest_analysis_v2_call_model_priority_3__sort_85 = Process(
        name = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_85",
        properties = ModelTransform(modelName = "Backtest_Analysis_v2_Call_Model_Priority_3__Sort_85")
    )
    textinput_19 >> backtest_analysis_v2_call_model_priority_3__unique_34._in(1)
    (
        backtest_analysis_v2_call_model_priority_3__join_57_inner._out(0)
        >> [backtest_analysis_v2_call_model_priority_3__alteryxselect_50._in(0),
              backtest_analysis_v2_call_model_priority_3__sort_5._in(0),
              backtest_analysis_v2_call_model_priority_3__summarize_17._in(0)]
    )
    (
        backtest_analysis_v2_call_model_priority_3__formula_93_1._out(0)
        >> [backtest_analysis_v2_call_model_priority_3__sort_99._in(0),
              backtest_analysis_v2_call_model_priority_3__sort_100._in(0),
              backtest_analysis_v2_call_model_priority_3__sort_104._in(0),
              backtest_analysis_v2_call_model_priority_3__sort_110._in(0),
              backtest_analysis_v2_call_model_priority_3__filter_258._in(0),
              backtest_analysis_v2_call_model_priority_3__summarize_315._in(0),
              backtest_analysis_v2_call_model_priority_3__numberofcallsto_231._in(0)]
    )
    (
        backtest_analysis_v2_call_model_priority_3__unique_34._out(0)
        >> [backtest_analysis_v2_call_model_priority_3__summarize_36._in(0),
              backtest_analysis_v2_call_model_priority_3__join_57_inner._in(3)]
    )
    (
        backtest_analysis_v2_call_model_priority_3__join_112_inner._out(0)
        >> [backtest_analysis_v2_call_model_priority_3__sort_223._in(0),
              backtest_analysis_v2_call_model_priority_3__formula_93_1._in(0)]
    )
    (
        backtest_analysis_v2_call_model_priority_3__filter_258._out(0)
        >> [backtest_analysis_v2_call_model_priority_3__sort_259._in(0),
              backtest_analysis_v2_call_model_priority_3__sort_263._in(0)]
    )
    (
        backtest_analysis_v2_call_model_priority_3__filter_2._out(0)
        >> [backtest_analysis_v2_call_model_priority_3__sort_7._in(0),
              backtest_analysis_v2_call_model_priority_3__sort_9._in(0),
              backtest_analysis_v2_call_model_priority_3__sort_15._in(0),
              backtest_analysis_v2_call_model_priority_3__summarize_16._in(0),
              backtest_analysis_v2_call_model_priority_3__join_112_inner._in(0),
              backtest_analysis_v2_call_model_priority_3__summarize_52._in(0)]
    )
    (
        backtest_analysis_v2_call_model_priority_3__alteryxselect_50._out(0)
        >> [backtest_analysis_v2_call_model_priority_3__filter_2._in(0),
              backtest_analysis_v2_call_model_priority_3__summarize_54._in(0),
              backtest_analysis_v2_call_model_priority_3__summarize_61._in(0),
              backtest_analysis_v2_call_model_priority_3__sort_85._in(0)]
    )
    (
        backtest_analysis_v2_call_model_priority_3__join_3_inner._out(0)
        >> [backtest_analysis_v2_call_model_priority_3__join_57_inner._in(5),
              backtest_analysis_v2_call_model_priority_3__sort_11._in(0),
              backtest_analysis_v2_call_model_priority_3__sort_13._in(0)]
    )
