from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Record_ID_sample_all",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'Record_ID_sample_all'")
)

with Pipeline(args) as pipeline:
    record_id_sample_all__recordid_124 = Process(
        name = "Record_ID_sample_all__RecordID_124",
        properties = ModelTransform(modelName = "Record_ID_sample_all__RecordID_124")
    )
    record_id_sample_all__recordid_125 = Process(
        name = "Record_ID_sample_all__RecordID_125",
        properties = ModelTransform(modelName = "Record_ID_sample_all__RecordID_125")
    )
    record_id_sample_all__recordid_138 = Process(
        name = "Record_ID_sample_all__RecordID_138",
        properties = ModelTransform(modelName = "Record_ID_sample_all__RecordID_138")
    )
    record_id_sample_all__textinput_123_cast = Process(
        name = "Record_ID_sample_all__TextInput_123_cast",
        properties = ModelTransform(modelName = "Record_ID_sample_all__TextInput_123_cast")
    )
    textinput_123 = Process(
        name = "TextInput_123",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_123", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_123 >> record_id_sample_all__textinput_123_cast
    (
        record_id_sample_all__textinput_123_cast._out(0)
        >> [record_id_sample_all__recordid_124._in(0), record_id_sample_all__recordid_125._in(0),
              record_id_sample_all__recordid_138._in(0)]
    )
