from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Billboard_Webscraping_App",
    version = 1,
    auto_layout = False,
    params = Parameters(
      Text_Box_42 = "'2023-12-15'",
      List_Box_48 = Expr("Value_2_Value,Rank,Value_1_Value,Song,Artist"),
      Question__Numeric_Up_Down_46 = 0,
      Question__List_Box_48 = "''",
      workflow_name = "'Billboard_Webscraping_App'",
      Question__Text_Box_42 = "''",
      Question__Check_Box_52 = "''"
    )
)

with Pipeline(args) as pipeline:
    billboard_webscraping_app__error_62 = Process(
        name = "Billboard_Webscraping_App__Error_62",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App__Error_62"),
        input_ports = None
    )
    billboard_webscraping_app__sample_44 = Process(
        name = "Billboard_Webscraping_App__Sample_44",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App__Sample_44")
    )
    billboard_webscraping_app__error_61 = Process(
        name = "Billboard_Webscraping_App__Error_61",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App__Error_61"),
        input_ports = None
    )
    billboard_webscraping_app__error_59 = Process(
        name = "Billboard_Webscraping_App__Error_59",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App__Error_59"),
        input_ports = None
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
    billboard_webscraping_app__error_60 = Process(
        name = "Billboard_Webscraping_App__Error_60",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App__Error_60"),
        input_ports = None
    )
    billboard_webscraping_app__error_58 = Process(
        name = "Billboard_Webscraping_App__Error_58",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App__Error_58"),
        input_ports = None
    )
    billboard_webscraping_app__error_64 = Process(
        name = "Billboard_Webscraping_App__Error_64",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App__Error_64"),
        input_ports = None
    )
    textinput_20 = Process(
        name = "TextInput_20",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_20", sourceType = "Seed")
        ),
        input_ports = None
    )
    billboard_webscraping_app__download_21_21_requestcomponents_0 = Process(
        name = "Billboard_Webscraping_App__Download_21_21_requestComponents_0",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App__Download_21_21_requestComponents_0")
    )
    billboard_webscraping_app__error_63 = Process(
        name = "Billboard_Webscraping_App__Error_63",
        properties = ModelTransform(modelName = "Billboard_Webscraping_App__Error_63"),
        input_ports = None
    )
    download_21_21 >> billboard_webscraping_app__sample_44
    textinput_20 >> billboard_webscraping_app__download_21_21_requestcomponents_0
    billboard_webscraping_app__download_21_21_requestcomponents_0 >> download_21_21
