from prophecy_analysis_sdk import *
meta_info = MetaInfo(pipeline_id = "p1")

with Analysis(meta_info = meta_info) as analysis:
    analysis.pie_chart(
        source = "", 
        title = "Charts Title", 
        y_axis_columns = [YAxisColumn(agg = AggregationType.NONE)], 
        show_legend = False, 
        outer_radius = 70
    )
