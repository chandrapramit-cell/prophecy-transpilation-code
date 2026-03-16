from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Record_ID_sample_all",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'Record_ID_sample_all'")
)

with Pipeline(args) as pipeline:
    record_id_sample_all__generate_record_id = Process(
        name = "Record_ID_sample_all__generate_record_id",
        properties = ModelTransform(modelName = "Record_ID_sample_all__generate_record_id")
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
    record_id_sample_all__incremental_recordid_group_region = Process(
        name = "Record_ID_sample_all__incremental_recordid_group_region",
        properties = ModelTransform(modelName = "Record_ID_sample_all__incremental_recordid_group_region")
    )
    record_id_sample_all__generate_incremental_id = Process(
        name = "Record_ID_sample_all__generate_incremental_id",
        properties = ModelTransform(modelName = "Record_ID_sample_all__generate_incremental_id")
    )
    textinput_123 >> record_id_sample_all__textinput_123_cast
    (
        record_id_sample_all__textinput_123_cast._out(0)
        >> [record_id_sample_all__generate_record_id._in(0), record_id_sample_all__generate_incremental_id._in(0),
              record_id_sample_all__incremental_recordid_group_region._in(0)]
    )
