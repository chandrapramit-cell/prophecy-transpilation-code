from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "pbasic", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    pbasic__multicolumnrename_1 = Process(
        name = "pbasic__MultiColumnRename_1",
        properties = ModelTransform(modelName = "pbasic__MultiColumnRename_1"),
        input_ports = None
    )
    pbasic__findduplicates_1 = Process(
        name = "pbasic__FindDuplicates_1",
        properties = ModelTransform(modelName = "pbasic__FindDuplicates_1"),
        input_ports = None
    )
    pbasic__sample_1 = Process(
        name = "pbasic__Sample_1",
        properties = ModelTransform(modelName = "pbasic__Sample_1"),
        input_ports = None
    )
    pbasic__datacleansing_1 = Process(
        name = "pbasic__DataCleansing_1",
        properties = ModelTransform(modelName = "pbasic__DataCleansing_1"),
        input_ports = None
    )
    pbasic__regex_1 = Process(
        name = "pbasic__Regex_1",
        properties = ModelTransform(modelName = "pbasic__Regex_1"),
        input_ports = None
    )
    pbasic__generaterows_1 = Process(
        name = "pbasic__GenerateRows_1",
        properties = ModelTransform(modelName = "pbasic__GenerateRows_1"),
        input_ports = None
    )
    pbasic__fuzzymatch_1 = Process(
        name = "pbasic__FuzzyMatch_1",
        properties = ModelTransform(modelName = "pbasic__FuzzyMatch_1"),
        input_ports = None
    )
    pbasic__dataencoderdecoder_1 = Process(
        name = "pbasic__DataEncoderDecoder_1",
        properties = ModelTransform(modelName = "pbasic__DataEncoderDecoder_1"),
        input_ports = None
    )
    pbasic__multicolumnedit_1 = Process(
        name = "pbasic__MultiColumnEdit_1",
        properties = ModelTransform(modelName = "pbasic__MultiColumnEdit_1"),
        input_ports = None
    )
    pbasic__countrecords_0 = Process(
        name = "pbasic__CountRecords_0",
        properties = ModelTransform(modelName = "pbasic__CountRecords_0"),
        input_ports = None
    )
    pbasic__datamasking_1 = Process(
        name = "pbasic__DataMasking_1",
        properties = ModelTransform(modelName = "pbasic__DataMasking_1"),
        input_ports = None
    )
    pbasic__dynamicselect_1 = Process(
        name = "pbasic__DynamicSelect_1",
        properties = ModelTransform(modelName = "pbasic__DynamicSelect_1"),
        input_ports = None
    )

