from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "intervention",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'intervention'")
)

with Pipeline(args) as pipeline:
    intervention__filter_874 = Process(
        name = "intervention__Filter_874",
        properties = ModelTransform(modelName = "intervention__Filter_874")
    )
    intervention__union_817 = Process(
        name = "intervention__Union_817",
        properties = ModelTransform(modelName = "intervention__Union_817"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__formula_782_0 = Process(
        name = "intervention__Formula_782_0",
        properties = ModelTransform(modelName = "intervention__Formula_782_0")
    )
    intervention__formula_814_0 = Process(
        name = "intervention__Formula_814_0",
        properties = ModelTransform(modelName = "intervention__Formula_814_0"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__summarize_777 = Process(
        name = "intervention__Summarize_777",
        properties = ModelTransform(modelName = "intervention__Summarize_777")
    )
    intervention__formula_1021_0 = Process(
        name = "intervention__Formula_1021_0",
        properties = ModelTransform(modelName = "intervention__Formula_1021_0")
    )
    intervention__filter_1020_reject = Process(
        name = "intervention__Filter_1020_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1020_reject")
    )
    intervention__join_834_left_unionleftouter = Process(
        name = "intervention__Join_834_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_834_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__filter_1656_reject = Process(
        name = "intervention__Filter_1656_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1656_reject")
    )
    intervention__filter_869 = Process(
        name = "intervention__Filter_869",
        properties = ModelTransform(modelName = "intervention__Filter_869")
    )
    intervention__join_850_left_unionleftouter = Process(
        name = "intervention__Join_850_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_850_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__filter_1020 = Process(
        name = "intervention__Filter_1020",
        properties = ModelTransform(modelName = "intervention__Filter_1020")
    )
    intervention__join_822_left_unionleftouter = Process(
        name = "intervention__Join_822_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_822_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__filter_870 = Process(
        name = "intervention__Filter_870",
        properties = ModelTransform(modelName = "intervention__Filter_870")
    )
    intervention__filter_876_reject = Process(
        name = "intervention__Filter_876_reject",
        properties = ModelTransform(modelName = "intervention__Filter_876_reject")
    )
    intervention__summarize_825 = Process(
        name = "intervention__Summarize_825",
        properties = ModelTransform(modelName = "intervention__Summarize_825")
    )
    intervention__filter_869_reject = Process(
        name = "intervention__Filter_869_reject",
        properties = ModelTransform(modelName = "intervention__Filter_869_reject")
    )
    intervention__filter_1019_reject = Process(
        name = "intervention__Filter_1019_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1019_reject")
    )
    intervention__join_800_left_unionleftouter = Process(
        name = "intervention__Join_800_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_800_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__union_866 = Process(
        name = "intervention__Union_866",
        properties = ModelTransform(modelName = "intervention__Union_866"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    intervention__filter_1649_reject = Process(
        name = "intervention__Filter_1649_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1649_reject")
    )
    intervention__filter_1650_reject = Process(
        name = "intervention__Filter_1650_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1650_reject")
    )
    intervention__filter_884 = Process(
        name = "intervention__Filter_884",
        properties = ModelTransform(modelName = "intervention__Filter_884")
    )
    intervention__intervention_li_765 = Process(
        name = "intervention__INTERVENTION_LI_765",
        properties = ModelTransform(modelName = "intervention__INTERVENTION_LI_765")
    )
    intervention__filter_877 = Process(
        name = "intervention__Filter_877",
        properties = ModelTransform(modelName = "intervention__Filter_877")
    )
    intervention__join_864_inner = Process(
        name = "intervention__Join_864_inner",
        properties = ModelTransform(modelName = "intervention__Join_864_inner"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__filter_873_reject = Process(
        name = "intervention__Filter_873_reject",
        properties = ModelTransform(modelName = "intervention__Filter_873_reject")
    )
    intervention__join_792_left_unionleftouter = Process(
        name = "intervention__Join_792_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_792_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__filter_816 = Process(
        name = "intervention__Filter_816",
        properties = ModelTransform(modelName = "intervention__Filter_816"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__multifieldformula_772 = Process(
        name = "intervention__MultiFieldFormula_772",
        properties = ModelTransform(modelName = "intervention__MultiFieldFormula_772")
    )
    intervention__union_784 = Process(
        name = "intervention__Union_784",
        properties = ModelTransform(modelName = "intervention__Union_784"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    intervention__filter_873 = Process(
        name = "intervention__Filter_873",
        properties = ModelTransform(modelName = "intervention__Filter_873")
    )
    intervention__filter_877_reject = Process(
        name = "intervention__Filter_877_reject",
        properties = ModelTransform(modelName = "intervention__Filter_877_reject")
    )
    intervention__summarize_801 = Process(
        name = "intervention__Summarize_801",
        properties = ModelTransform(modelName = "intervention__Summarize_801")
    )
    intervention__alteryxselect_890 = Process(
        name = "intervention__AlteryxSelect_890",
        properties = ModelTransform(modelName = "intervention__AlteryxSelect_890"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__join_815_inner = Process(
        name = "intervention__Join_815_inner",
        properties = ModelTransform(modelName = "intervention__Join_815_inner"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__filter_870_reject = Process(
        name = "intervention__Filter_870_reject",
        properties = ModelTransform(modelName = "intervention__Filter_870_reject")
    )
    intervention__summarize_826 = Process(
        name = "intervention__Summarize_826",
        properties = ModelTransform(modelName = "intervention__Summarize_826")
    )
    intervention__alteryxselect_860 = Process(
        name = "intervention__AlteryxSelect_860",
        properties = ModelTransform(modelName = "intervention__AlteryxSelect_860")
    )
    intervention__unique_1018 = Process(
        name = "intervention__Unique_1018",
        properties = ModelTransform(modelName = "intervention__Unique_1018"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    intervention__summarize_820 = Process(
        name = "intervention__Summarize_820",
        properties = ModelTransform(modelName = "intervention__Summarize_820")
    )
    intervention__filter_876 = Process(
        name = "intervention__Filter_876",
        properties = ModelTransform(modelName = "intervention__Filter_876")
    )
    intervention__filter_883 = Process(
        name = "intervention__Filter_883",
        properties = ModelTransform(modelName = "intervention__Filter_883")
    )
    intervention__filter_1659 = Process(
        name = "intervention__Filter_1659",
        properties = ModelTransform(modelName = "intervention__Filter_1659")
    )
    intervention__summarize_785 = Process(
        name = "intervention__Summarize_785",
        properties = ModelTransform(modelName = "intervention__Summarize_785")
    )
    intervention__filter_878 = Process(
        name = "intervention__Filter_878",
        properties = ModelTransform(modelName = "intervention__Filter_878")
    )
    intervention__join_798_left_unionleftouter = Process(
        name = "intervention__Join_798_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_798_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__summarize_771 = Process(
        name = "intervention__Summarize_771",
        properties = ModelTransform(modelName = "intervention__Summarize_771")
    )
    intervention__filter_872_reject = Process(
        name = "intervention__Filter_872_reject",
        properties = ModelTransform(modelName = "intervention__Filter_872_reject")
    )
    intervention__filter_872 = Process(
        name = "intervention__Filter_872",
        properties = ModelTransform(modelName = "intervention__Filter_872")
    )
    intervention__summarize_819 = Process(
        name = "intervention__Summarize_819",
        properties = ModelTransform(modelName = "intervention__Summarize_819")
    )
    intervention__filter_880_reject = Process(
        name = "intervention__Filter_880_reject",
        properties = ModelTransform(modelName = "intervention__Filter_880_reject")
    )
    intervention__filter_1659_reject = Process(
        name = "intervention__Filter_1659_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1659_reject")
    )
    intervention__filter_1652 = Process(
        name = "intervention__Filter_1652",
        properties = ModelTransform(modelName = "intervention__Filter_1652")
    )
    intervention__filter_879 = Process(
        name = "intervention__Filter_879",
        properties = ModelTransform(modelName = "intervention__Filter_879")
    )
    intervention__filter_874_reject = Process(
        name = "intervention__Filter_874_reject",
        properties = ModelTransform(modelName = "intervention__Filter_874_reject")
    )
    intervention__filter_880 = Process(
        name = "intervention__Filter_880",
        properties = ModelTransform(modelName = "intervention__Filter_880")
    )
    intervention__filter_1655_reject = Process(
        name = "intervention__Filter_1655_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1655_reject")
    )
    intervention__filter_871_reject = Process(
        name = "intervention__Filter_871_reject",
        properties = ModelTransform(modelName = "intervention__Filter_871_reject")
    )
    intervention__filter_1656 = Process(
        name = "intervention__Filter_1656",
        properties = ModelTransform(modelName = "intervention__Filter_1656")
    )
    intervention__join_776_left_unionfullouter = Process(
        name = "intervention__Join_776_left_UnionFullOuter",
        properties = ModelTransform(modelName = "intervention__Join_776_left_UnionFullOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__filter_875_reject = Process(
        name = "intervention__Filter_875_reject",
        properties = ModelTransform(modelName = "intervention__Filter_875_reject")
    )
    intervention__filter_871 = Process(
        name = "intervention__Filter_871",
        properties = ModelTransform(modelName = "intervention__Filter_871")
    )
    intervention__filter_1650 = Process(
        name = "intervention__Filter_1650",
        properties = ModelTransform(modelName = "intervention__Filter_1650")
    )
    intervention__filter_1019 = Process(
        name = "intervention__Filter_1019",
        properties = ModelTransform(modelName = "intervention__Filter_1019")
    )
    intervention__filter_1652_reject = Process(
        name = "intervention__Filter_1652_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1652_reject")
    )
    intervention__alteryxselect_838 = Process(
        name = "intervention__AlteryxSelect_838",
        properties = ModelTransform(modelName = "intervention__AlteryxSelect_838")
    )
    intervention__summarize_824 = Process(
        name = "intervention__Summarize_824",
        properties = ModelTransform(modelName = "intervention__Summarize_824")
    )
    intervention__filter_875 = Process(
        name = "intervention__Filter_875",
        properties = ModelTransform(modelName = "intervention__Filter_875")
    )
    intervention__summarize_818 = Process(
        name = "intervention__Summarize_818",
        properties = ModelTransform(modelName = "intervention__Summarize_818")
    )
    intervention__filter_878_reject = Process(
        name = "intervention__Filter_878_reject",
        properties = ModelTransform(modelName = "intervention__Filter_878_reject")
    )
    intervention__filter_884_reject = Process(
        name = "intervention__Filter_884_reject",
        properties = ModelTransform(modelName = "intervention__Filter_884_reject")
    )
    intervention__filter_1649 = Process(
        name = "intervention__Filter_1649",
        properties = ModelTransform(modelName = "intervention__Filter_1649")
    )
    intervention__summarize_786 = Process(
        name = "intervention__Summarize_786",
        properties = ModelTransform(modelName = "intervention__Summarize_786")
    )
    intervention__summarize_779 = Process(
        name = "intervention__Summarize_779",
        properties = ModelTransform(modelName = "intervention__Summarize_779")
    )
    intervention__filter_879_reject = Process(
        name = "intervention__Filter_879_reject",
        properties = ModelTransform(modelName = "intervention__Filter_879_reject")
    )
    intervention__filter_883_reject = Process(
        name = "intervention__Filter_883_reject",
        properties = ModelTransform(modelName = "intervention__Filter_883_reject")
    )
    intervention__join_805_left_unionleftouter = Process(
        name = "intervention__Join_805_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_805_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__filter_1655 = Process(
        name = "intervention__Filter_1655",
        properties = ModelTransform(modelName = "intervention__Filter_1655")
    )
    (
        intervention__join_805_left_unionleftouter._out(0)
        >> [intervention__summarize_801._in(0), intervention__filter_873._in(0),
              intervention__filter_873_reject._in(0), intervention__join_822_left_unionleftouter._in(1)]
    )
    (
        intervention__alteryxselect_838._out(0)
        >> [intervention__filter_878._in(0), intervention__filter_878_reject._in(0),
              intervention__alteryxselect_890._in(2)]
    )
    (
        intervention__alteryxselect_890._out(0)
        >> [intervention__filter_1020._in(0), intervention__filter_1020_reject._in(0),
              intervention__formula_1021_0._in(0)]
    )
    (
        intervention__join_800_left_unionleftouter._out(0)
        >> [intervention__filter_874._in(0), intervention__filter_874_reject._in(0),
              intervention__join_805_left_unionleftouter._in(2)]
    )
    (
        intervention__join_792_left_unionleftouter._out(0)
        >> [intervention__filter_871._in(0), intervention__filter_871_reject._in(0),
              intervention__join_798_left_unionleftouter._in(2)]
    )
    (
        intervention__join_850_left_unionleftouter._out(0)
        >> [intervention__filter_1649._in(0), intervention__filter_1649_reject._in(0),
              intervention__multifieldformula_772._in(0)]
    )
    (
        intervention__union_817._out(0)
        >> [intervention__summarize_826._in(0), intervention__summarize_820._in(0),
              intervention__summarize_819._in(0), intervention__summarize_818._in(0),
              intervention__filter_877._in(0), intervention__filter_877_reject._in(0),
              intervention__filter_1656._in(0), intervention__filter_1656_reject._in(0),
              intervention__alteryxselect_838._in(0)]
    )
    (
        intervention__union_866._out(0)
        >> [intervention__filter_883._in(0), intervention__filter_883_reject._in(0),
              intervention__join_776_left_unionfullouter._in(1)]
    )
    (
        intervention__join_864_inner._out(0)
        >> [intervention__union_866._in(3), intervention__filter_884._in(0), intervention__filter_884_reject._in(0)]
    )
    (
        intervention__formula_1021_0._out(0)
        >> [intervention__intervention_li_765._in(0), intervention__filter_1659._in(0),
              intervention__filter_1659_reject._in(0)]
    )
    (
        intervention__multifieldformula_772._out(0)
        >> [intervention__unique_1018._in(2), intervention__summarize_771._in(0)]
    )
    (
        intervention__unique_1018._out(0)
        >> [intervention__filter_1019._in(0), intervention__filter_1019_reject._in(0),
              intervention__filter_1650._in(0), intervention__filter_1650_reject._in(0),
              intervention__union_866._in(1), intervention__join_864_inner._in(1)]
    )
    intervention__alteryxselect_860._out(0) >> [intervention__union_866._in(0), intervention__join_864_inner._in(0)]
    (
        intervention__join_822_left_unionleftouter._out(0)
        >> [intervention__summarize_825._in(0), intervention__filter_875._in(0),
              intervention__filter_875_reject._in(0), intervention__join_834_left_unionleftouter._in(1)]
    )
    (
        intervention__join_798_left_unionleftouter._out(0)
        >> [intervention__filter_872._in(0), intervention__filter_872_reject._in(0),
              intervention__join_800_left_unionleftouter._in(2)]
    )
    (
        intervention__union_784._out(0)
        >> [intervention__summarize_785._in(0), intervention__filter_870._in(0),
              intervention__filter_870_reject._in(0), intervention__join_792_left_unionleftouter._in(2)]
    )
    (
        intervention__join_815_inner._out(0)
        >> [intervention__union_817._in(1), intervention__filter_880._in(0), intervention__filter_880_reject._in(0)]
    )
    (
        intervention__join_776_left_unionfullouter._out(0)
        >> [intervention__union_784._in(0), intervention__union_784._in(1), intervention__summarize_779._in(0),
              intervention__summarize_777._in(0), intervention__filter_869._in(0),
              intervention__filter_869_reject._in(0)]
    )
    (
        intervention__join_834_left_unionleftouter._out(0)
        >> [intervention__filter_816._in(0), intervention__join_815_inner._in(0), intervention__filter_876._in(0),
              intervention__filter_876_reject._in(0)]
    )
    (
        intervention__formula_782_0._out(0)
        >> [intervention__summarize_786._in(0), intervention__union_784._in(2), intervention__union_784._in(3)]
    )
    (
        intervention__formula_814_0._out(0)
        >> [intervention__filter_1652._in(0), intervention__filter_1652_reject._in(0),
              intervention__filter_816._in(1), intervention__join_815_inner._in(1)]
    )
    (
        intervention__filter_816._out(0)
        >> [intervention__union_817._in(0), intervention__filter_879._in(0), intervention__filter_879_reject._in(0),
              intervention__filter_1655._in(0), intervention__filter_1655_reject._in(0)]
    )
