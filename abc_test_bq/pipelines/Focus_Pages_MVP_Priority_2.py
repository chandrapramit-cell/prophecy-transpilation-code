from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Focus_Pages_MVP_Priority_2",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_aka_alxaa2_Quer_116 = "''",
      username_aka_alxaa2_Quer_53 = "''",
      password_aka_alxaa2_Quer_167 = "''",
      password_aka_alxaa2_Quer_1 = "''",
      password_aka_alxaa2_Quer_55 = "''",
      username_aka_alxaa2_Quer_57 = "''",
      password_aka_alxaa2_Quer_57 = "''",
      password_aka_alxaa2_Quer_119 = "''",
      password_aka_alxaa2_Quer_116 = "''",
      workflow_name = "'Focus_Pages_MVP_Priority_2'",
      username_aka_alxaa2_Quer_119 = "''",
      password_aka_alxaa2_Quer_54 = "''",
      username_aka_alxaa2_Quer_56 = "''",
      password_aka_alxaa2_Quer_134 = "''",
      password_aka_alxaa2_Quer_58 = "''",
      username_aka_alxaa2_Quer_98 = "''",
      username_aka_alxaa2_Quer_29 = "''",
      password_aka_alxaa2_Quer_29 = "''",
      username_aka_alxaa2_Quer_167 = "''",
      password_aka_alxaa2_Quer_98 = "''",
      password_aka_alxaa2_Quer_53 = "''",
      username_aka_alxaa2_Quer_55 = "''",
      username_aka_alxaa2_Quer_134 = "''",
      username_aka_alxaa2_Quer_54 = "''",
      username_aka_alxaa2_Quer_58 = "''",
      username_aka_alxaa2_Quer_1 = "''",
      password_aka_alxaa2_Quer_56 = "''"
    )
)

with Pipeline(args) as pipeline:
    focus_pages_mvp_priority_2__summarize_113 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_113",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_113")
    )
    focus_pages_mvp_priority_2__formula_101_1 = Process(
        name = "Focus_Pages_MVP_Priority_2__Formula_101_1",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Formula_101_1")
    )
    focus_pages_mvp_priority_2__summarize_153 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_153",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_153")
    )
    focus_pages_mvp_priority_2__filter_93 = Process(
        name = "Focus_Pages_MVP_Priority_2__Filter_93",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Filter_93")
    )
    focus_pages_mvp_priority_2__summarize_142 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_142",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_142")
    )
    focus_pages_mvp_priority_2__shoulder_surger_223 = Process(
        name = "Focus_Pages_MVP_Priority_2__shoulder_surger_223",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__shoulder_surger_223")
    )
    focus_pages_mvp_priority_2__pregnancy_compl_170 = Process(
        name = "Focus_Pages_MVP_Priority_2__pregnancy_compl_170",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__pregnancy_compl_170")
    )
    focus_pages_mvp_priority_2__formula_102_1 = Process(
        name = "Focus_Pages_MVP_Priority_2__Formula_102_1",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Formula_102_1")
    )
    focus_pages_mvp_priority_2__unique_110 = Process(
        name = "Focus_Pages_MVP_Priority_2__Unique_110",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Unique_110")
    )
    focus_pages_mvp_priority_2__join_109_inner_formula_0 = Process(
        name = "Focus_Pages_MVP_Priority_2__Join_109_inner_formula_0",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Join_109_inner_formula_0"),
        input_ports = ["in_0", "in_1"]
    )
    focus_pages_mvp_priority_2__summarize_141 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_141",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_141")
    )
    focus_pages_mvp_priority_2__pregnancy_comp__236 = Process(
        name = "Focus_Pages_MVP_Priority_2__pregnancy_comp__236",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__pregnancy_comp__236")
    )
    focus_pages_mvp_priority_2__neck_surgery_cs_227 = Process(
        name = "Focus_Pages_MVP_Priority_2__neck_surgery_cs_227",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__neck_surgery_cs_227")
    )
    focus_pages_mvp_priority_2__summarize_46 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_46",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_46")
    )
    focus_pages_mvp_priority_2__join_126_inner = Process(
        name = "Focus_Pages_MVP_Priority_2__Join_126_inner",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Join_126_inner"),
        input_ports = ["in_0", "in_1"]
    )
    focus_pages_mvp_priority_2__filter_155 = Process(
        name = "Focus_Pages_MVP_Priority_2__Filter_155",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Filter_155")
    )
    focus_pages_mvp_priority_2__summarize_94 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_94",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_94")
    )
    focus_pages_mvp_priority_2__summarize_152 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_152",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_152")
    )
    focus_pages_mvp_priority_2__knee_replacemen_231 = Process(
        name = "Focus_Pages_MVP_Priority_2__knee_replacemen_231",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__knee_replacemen_231")
    )
    focus_pages_mvp_priority_2__union_103 = Process(
        name = "Focus_Pages_MVP_Priority_2__Union_103",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Union_103"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    focus_pages_mvp_priority_2__msk_members_csv_72 = Process(
        name = "Focus_Pages_MVP_Priority_2__msk_members_csv_72",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__msk_members_csv_72"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    focus_pages_mvp_priority_2__join_135_inner_unionleftouter = Process(
        name = "Focus_Pages_MVP_Priority_2__Join_135_inner_UnionLeftOuter",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Join_135_inner_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    focus_pages_mvp_priority_2__mother_birthing_199 = Process(
        name = "Focus_Pages_MVP_Priority_2__mother_birthing_199",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__mother_birthing_199")
    )
    focus_pages_mvp_priority_2__summarize_144 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_144",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_144")
    )
    focus_pages_mvp_priority_2__high_risk_preg__237 = Process(
        name = "Focus_Pages_MVP_Priority_2__high_risk_preg__237",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__high_risk_preg__237")
    )
    focus_pages_mvp_priority_2__oncology_cache__21 = Process(
        name = "Focus_Pages_MVP_Priority_2__oncology_cache__21",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__oncology_cache__21")
    )
    focus_pages_mvp_priority_2__unique_127 = Process(
        name = "Focus_Pages_MVP_Priority_2__Unique_127",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Unique_127")
    )
    focus_pages_mvp_priority_2__back_surgery_cs_219 = Process(
        name = "Focus_Pages_MVP_Priority_2__back_surgery_cs_219",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__back_surgery_cs_219")
    )
    focus_pages_mvp_priority_2__unique_106 = Process(
        name = "Focus_Pages_MVP_Priority_2__Unique_106",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Unique_106"),
        input_ports = ["in_0", "in_1"]
    )
    focus_pages_mvp_priority_2__summarize_82 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_82",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_82")
    )
    focus_pages_mvp_priority_2__sort_39 = Process(
        name = "Focus_Pages_MVP_Priority_2__Sort_39",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Sort_39")
    )
    focus_pages_mvp_priority_2__alteryxselect_133 = Process(
        name = "Focus_Pages_MVP_Priority_2__AlteryxSelect_133",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__AlteryxSelect_133")
    )
    focus_pages_mvp_priority_2__diabetes_csv_47 = Process(
        name = "Focus_Pages_MVP_Priority_2__diabetes_csv_47",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__diabetes_csv_47")
    )
    focus_pages_mvp_priority_2__summarize_111 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_111",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_111")
    )
    focus_pages_mvp_priority_2__summarize_96 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_96",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_96")
    )
    focus_pages_mvp_priority_2__pregnancy_membe_162 = Process(
        name = "Focus_Pages_MVP_Priority_2__pregnancy_membe_162",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__pregnancy_membe_162")
    )
    focus_pages_mvp_priority_2__summarize_151 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_151",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_151")
    )
    focus_pages_mvp_priority_2__summarize_95 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_95",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_95")
    )
    focus_pages_mvp_priority_2__summarize_145 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_145",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_145")
    )
    focus_pages_mvp_priority_2__join_107_left = Process(
        name = "Focus_Pages_MVP_Priority_2__Join_107_left",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Join_107_left"),
        input_ports = ["in_0", "in_1"]
    )
    focus_pages_mvp_priority_2__summarize_156 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_156",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_156")
    )
    focus_pages_mvp_priority_2__formula_90_0 = Process(
        name = "Focus_Pages_MVP_Priority_2__Formula_90_0",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Formula_90_0"),
        input_ports = ["in_0", "in_1"]
    )
    focus_pages_mvp_priority_2__cancer_pt_csv_198 = Process(
        name = "Focus_Pages_MVP_Priority_2__cancer_pt_csv_198",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__cancer_pt_csv_198"),
        input_ports = ["in_0", "in_1"]
    )
    focus_pages_mvp_priority_2__join_107_inner = Process(
        name = "Focus_Pages_MVP_Priority_2__Join_107_inner",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Join_107_inner"),
        input_ports = ["in_0", "in_1"]
    )
    focus_pages_mvp_priority_2__sort_160 = Process(
        name = "Focus_Pages_MVP_Priority_2__Sort_160",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Sort_160")
    )
    focus_pages_mvp_priority_2__hip_proc_csv_235 = Process(
        name = "Focus_Pages_MVP_Priority_2__hip_proc_csv_235",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__hip_proc_csv_235")
    )
    focus_pages_mvp_priority_2__filter_92 = Process(
        name = "Focus_Pages_MVP_Priority_2__Filter_92",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Filter_92")
    )
    focus_pages_mvp_priority_2__unique_125 = Process(
        name = "Focus_Pages_MVP_Priority_2__Unique_125",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Unique_125"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    focus_pages_mvp_priority_2__summarize_154 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_154",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_154")
    )
    focus_pages_mvp_priority_2__vague_msk_csv_215 = Process(
        name = "Focus_Pages_MVP_Priority_2__vague_msk_csv_215",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__vague_msk_csv_215")
    )
    focus_pages_mvp_priority_2__diabetes_cache__30 = Process(
        name = "Focus_Pages_MVP_Priority_2__diabetes_cache__30",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__diabetes_cache__30")
    )
    focus_pages_mvp_priority_2__filter_157_to_filter_97 = Process(
        name = "Focus_Pages_MVP_Priority_2__Filter_157_to_Filter_97",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Filter_157_to_Filter_97")
    )
    focus_pages_mvp_priority_2__summarize_143 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_143",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_143")
    )
    focus_pages_mvp_priority_2__filter_114 = Process(
        name = "Focus_Pages_MVP_Priority_2__Filter_114",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Filter_114")
    )
    focus_pages_mvp_priority_2__rrclm_f_csv_207 = Process(
        name = "Focus_Pages_MVP_Priority_2__rrclm_f_csv_207",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__rrclm_f_csv_207")
    )
    focus_pages_mvp_priority_2__diag_cd_desc_cs_211 = Process(
        name = "Focus_Pages_MVP_Priority_2__diag_cd_desc_cs_211",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__diag_cd_desc_cs_211")
    )
    focus_pages_mvp_priority_2__sort_150 = Process(
        name = "Focus_Pages_MVP_Priority_2__Sort_150",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Sort_150")
    )
    focus_pages_mvp_priority_2__formula_80_1 = Process(
        name = "Focus_Pages_MVP_Priority_2__Formula_80_1",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Formula_80_1")
    )
    focus_pages_mvp_priority_2__summarize_146 = Process(
        name = "Focus_Pages_MVP_Priority_2__Summarize_146",
        properties = ModelTransform(modelName = "Focus_Pages_MVP_Priority_2__Summarize_146")
    )
    (
        focus_pages_mvp_priority_2__join_107_left._out(0)
        >> [focus_pages_mvp_priority_2__summarize_145._in(0), focus_pages_mvp_priority_2__alteryxselect_133._in(0)]
    )
    (
        focus_pages_mvp_priority_2__join_109_inner_formula_0._out(0)
        >> [focus_pages_mvp_priority_2__summarize_151._in(0),
              focus_pages_mvp_priority_2__join_135_inner_unionleftouter._in(2)]
    )
    (
        focus_pages_mvp_priority_2__sort_39._out(0)
        >> [focus_pages_mvp_priority_2__summarize_46._in(0), focus_pages_mvp_priority_2__formula_80_1._in(0)]
    )
    (
        focus_pages_mvp_priority_2__unique_106._out(0)
        >> [focus_pages_mvp_priority_2__join_107_left._in(1), focus_pages_mvp_priority_2__join_107_inner._in(1)]
    )
    (
        focus_pages_mvp_priority_2__join_107_inner._out(0)
        >> [focus_pages_mvp_priority_2__summarize_146._in(0), focus_pages_mvp_priority_2__sort_150._in(0)]
    )
    (
        focus_pages_mvp_priority_2__join_135_inner_unionleftouter._out(0)
        >> [focus_pages_mvp_priority_2__unique_110._in(0), focus_pages_mvp_priority_2__summarize_152._in(0)]
    )
    (
        focus_pages_mvp_priority_2__unique_127._out(0)
        >> [focus_pages_mvp_priority_2__summarize_111._in(0), focus_pages_mvp_priority_2__pregnancy_membe_162._in(0),
              focus_pages_mvp_priority_2__summarize_154._in(0)]
    )
    (
        focus_pages_mvp_priority_2__formula_101_1._out(0)
        >> [focus_pages_mvp_priority_2__union_103._in(1), focus_pages_mvp_priority_2__summarize_141._in(0)]
    )
    (
        focus_pages_mvp_priority_2__formula_80_1._out(0)
        >> [focus_pages_mvp_priority_2__summarize_82._in(0), focus_pages_mvp_priority_2__diabetes_csv_47._in(0)]
    )
    (
        focus_pages_mvp_priority_2__filter_93._out(0)
        >> [focus_pages_mvp_priority_2__union_103._in(3), focus_pages_mvp_priority_2__summarize_95._in(0)]
    )
    (
        focus_pages_mvp_priority_2__filter_114._out(0)
        >> [focus_pages_mvp_priority_2__unique_106._in(0), focus_pages_mvp_priority_2__summarize_113._in(0)]
    )
    (
        focus_pages_mvp_priority_2__unique_125._out(0)
        >> [focus_pages_mvp_priority_2__join_109_inner_formula_0._in(0),
              focus_pages_mvp_priority_2__join_126_inner._in(0)]
    )
    (
        focus_pages_mvp_priority_2__unique_110._out(0)
        >> [focus_pages_mvp_priority_2__unique_127._in(0), focus_pages_mvp_priority_2__summarize_153._in(0)]
    )
    (
        focus_pages_mvp_priority_2__formula_90_0._out(0)
        >> [focus_pages_mvp_priority_2__filter_92._in(0), focus_pages_mvp_priority_2__filter_93._in(0),
              focus_pages_mvp_priority_2__filter_114._in(0), focus_pages_mvp_priority_2__sort_160._in(0),
              focus_pages_mvp_priority_2__filter_157_to_filter_97._in(0),
              focus_pages_mvp_priority_2__formula_102_1._in(0)]
    )
    (
        focus_pages_mvp_priority_2__formula_102_1._out(0)
        >> [focus_pages_mvp_priority_2__summarize_142._in(0), focus_pages_mvp_priority_2__filter_155._in(0)]
    )
    (
        focus_pages_mvp_priority_2__filter_155._out(0)
        >> [focus_pages_mvp_priority_2__union_103._in(0), focus_pages_mvp_priority_2__summarize_156._in(0)]
    )
    (
        focus_pages_mvp_priority_2__alteryxselect_133._out(0)
        >> [focus_pages_mvp_priority_2__summarize_143._in(0),
              focus_pages_mvp_priority_2__join_109_inner_formula_0._in(1),
              focus_pages_mvp_priority_2__join_126_inner._in(1)]
    )
    (
        focus_pages_mvp_priority_2__union_103._out(0)
        >> [focus_pages_mvp_priority_2__summarize_144._in(0), focus_pages_mvp_priority_2__join_107_left._in(0),
              focus_pages_mvp_priority_2__join_107_inner._in(0)]
    )
    (
        focus_pages_mvp_priority_2__filter_157_to_filter_97._out(0)
        >> [focus_pages_mvp_priority_2__unique_106._in(1), focus_pages_mvp_priority_2__summarize_96._in(0)]
    )
    (
        focus_pages_mvp_priority_2__filter_92._out(0)
        >> [focus_pages_mvp_priority_2__summarize_94._in(0), focus_pages_mvp_priority_2__formula_101_1._in(0)]
    )
