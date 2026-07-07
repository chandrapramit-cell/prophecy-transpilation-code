from prophecy_analysis_sdk import *
meta_info = MetaInfo(pipeline_id = "CEM_Securities_PruVal_6m_Trade_Volume_57")

with Analysis(meta_info = meta_info) as analysis:
    analysis.text(
        text = "# CEM Securities Trade Analysis Dashboard\n\nComprehensive visualization of credit trading data across regions, products, and time periods."
    )
    analysis.metric_chart(
        source = "viz_metrics_summary", 
        title = "Total Notional vs Consideration (USD)", 
        metric_config = MetricConfig(
          metric_column = "total_notional", 
          metric_agg = AggregationType.NONE, 
          reference_column = "total_consideration", 
          reference_agg = AggregationType.NONE
        )
    )
    analysis.bar_chart(
        source = "viz_trades_by_region", 
        title = "Trade Volume by Region", 
        x_axis_column = "Region", 
        y_axis_columns = [         YAxisColumn(
           name = "trade_count", 
           agg = AggregationType.NONE, 
           color = "#4c4ddc"
         ),          YAxisColumn(
           name = "total_notional_usd", 
           agg = AggregationType.NONE, 
           color = "#088ab2"
         )], 
        y_axis_config = None
    )
    analysis.pie_chart(
        source = "viz_trades_by_product_type", 
        title = "Trade Distribution by Product Type", 
        x_axis_column = "product_type", 
        y_axis_columns = [         YAxisColumn(
           name = "trade_count", 
           agg = AggregationType.NONE, 
           color = "#4c4ddc"
         )], 
        chart_style = PieStyle.DONUT
    )
    analysis.line_chart(
        source = "viz_trades_by_date", 
        title = "Trade Activity Over Time", 
        x_axis_column = "trade_date", 
        y_axis_columns = [         YAxisColumn(
           name = "trade_count", 
           agg = AggregationType.NONE, 
           color = "#4c4ddc"
         ),          YAxisColumn(
           name = "total_notional_usd", 
           agg = AggregationType.NONE, 
           color = "#088ab2"
         )], 
        y_axis_config = None
    )
    analysis.area_chart(
        source = "viz_trades_by_date", 
        title = "Notional Trend Over Time", 
        x_axis_column = "trade_date", 
        y_axis_columns = [         YAxisColumn(
           name = "total_notional_usd", 
           agg = AggregationType.NONE, 
           color = "#4c4ddc"
         )], 
        y_axis_config = None
    )
    analysis.heatmap_chart(
        source = "viz_region_product_heatmap", 
        title = "Trade Intensity: Region vs Product Type", 
        x_axis_column = "product_type", 
        y_axis_column = "Region", 
        value_column = "trade_count"
    )
    analysis.funnel_chart(
        source = "viz_trades_by_status", 
        title = "Trade Distribution by Status", 
        x_axis_column = "funnel_stage", 
        y_axis_columns = [         YAxisColumn(
           name = "trade_count", 
           agg = AggregationType.NONE, 
           color = "#4c4ddc"
         )], 
        gap = 10
    )
    analysis.sankey_chart(
        source = "viz_region_desk_flow", 
        title = "Trade Flow: Region to Desk", 
        source_column = "source_node", 
        target_column = "target_node", 
        value_column = "flow_value"
    )
    analysis.scatter_chart(
        source = "viz_scatter_data", 
        title = "Notional vs Consideration Correlation", 
        series = [         ScatterSeriesConfig(
           x_axis_column = "notional_usd", 
           y_axis_column = "consideration_usd", 
           size = 10, 
           label = "trade_id", 
           color = "#4c4ddc"
         )]
    )
    analysis.data_preview(
        source = "viz_trades_by_region", 
        columns = [Column(column = "Region"),  Column(column = "trade_count", label = "Number of Trades"),          Column(column = "total_notional_usd", label = "Total Notional (USD)"),          Column(column = "total_consideration_usd", label = "Total Consideration (USD)")], 
        title = "Trade Summary by Region"
    )
