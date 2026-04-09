from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Billboard_Webscraping_App_1_",
    version = 1,
    auto_layout = False,
    params = Parameters(
      INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__ = "'2023-12-15'",
      SELECT_WHICH_FIELDS_YOU_WOULD_LIKE_TO_SEE_DISPLAYED = "''",
      SELECT_HOW_MANY_RESULTS_YOU_WOULD_LIKE_TO_SEE = "''",
      START_FROM_BOTTOM_OF_THE_CHART = "''",
      VARIABLE20_DATA_R_C = "concat('https://www.billboard.com/charts/hot-100/', Input_a_date_to_see_corresponding_Billboard_Hot_100_data__)",
      VARIABLE44_N = Expr("{{ var('Select_how_many_results_you_would_like_to_see') }}"),
      VARIABLE44_MODE = "'Last'",
      WORKFLOW_NAME = "'Billboard_Webscraping_App_1_'",
      QUESTION__TEXT_BOX_42 = "''",
      QUESTION__NUMERIC_UP_DOWN_46 = 0,
      QUESTION__LIST_BOX_48 = "''",
      QUESTION__CHECK_BOX_52 = "''"
    )
)

with Pipeline(args) as pipeline:
    billboard_webscraping_app_1___download_21_21_requestcomponents_0 = Process(
        name = "Billboard_Webscraping_App_1___Download_21_21_requestComponents_0",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App_1___Download_21_21_requestComponents_0")
    )
    billboard_webscraping_app_1___error_58 = Process(
        name = "Billboard_Webscraping_App_1___Error_58",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App_1___Error_58"),
        input_ports = None
    )
    billboard_webscraping_app_1___error_59 = Process(
        name = "Billboard_Webscraping_App_1___Error_59",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App_1___Error_59"),
        input_ports = None
    )
    billboard_webscraping_app_1___error_60 = Process(
        name = "Billboard_Webscraping_App_1___Error_60",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App_1___Error_60"),
        input_ports = None
    )
    billboard_webscraping_app_1___error_61 = Process(
        name = "Billboard_Webscraping_App_1___Error_61",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App_1___Error_61"),
        input_ports = None
    )
    billboard_webscraping_app_1___error_62 = Process(
        name = "Billboard_Webscraping_App_1___Error_62",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App_1___Error_62"),
        input_ports = None
    )
    billboard_webscraping_app_1___error_63 = Process(
        name = "Billboard_Webscraping_App_1___Error_63",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App_1___Error_63"),
        input_ports = None
    )
    billboard_webscraping_app_1___error_64 = Process(
        name = "Billboard_Webscraping_App_1___Error_64",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App_1___Error_64"),
        input_ports = None
    )
    billboard_webscraping_app_1___sample_44 = Process(
        name = "Billboard_Webscraping_App_1___Sample_44",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App_1___Sample_44")
    )
    download_21_21 = Process(
        name = "Download_21_21",
        properties = RestAPI(
          method = "GET",
          body = "",
          url = "{{ _processedUrl }}",
          targetColumnName = "",
          params = [],
          authType = "",
          headers = [],
          parseAPIResponse = False,
          credentials = None
        )
    )
    textinput_20 = Process(
        name = "TextInput_20",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_20", sourceType = "Seed")
        ),
        input_ports = None
    )
    download_21_21 >> billboard_webscraping_app_1___sample_44
    textinput_20 >> billboard_webscraping_app_1___download_21_21_requestcomponents_0
    billboard_webscraping_app_1___download_21_21_requestcomponents_0 >> download_21_21
