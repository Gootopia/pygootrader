# Custom packages
from charts import Chart, PlotAttribute, Colormap
from datasources import PolygonIO, DataSourceHelpers
from influx_database import InfluxDatabase, InfluxQuery
from tags import InstrumentTags
from technical_analysis import MovingAverage, GooEmaDelta


if __name__ == "__main__":   
    try:
        ticker = "SPY"
        #ds_polygon = PolygonIO()
        #data = ds_polygon.download_data(ticker, "2022-01-01")
        #DataSourceHelpers.display_ohlc(data,ticker)
        
        db = InfluxDatabase()
        tags = InstrumentTags(symbol=ticker)
        #db.write_pandas(dataframe=data,tags=tags)

        tags_spy = InstrumentTags(symbol="schb")
        query = InfluxQuery().range().add_tag_group(tags_spy).build(db)
        data_df = db.read_records(query)
        
        ema_200 = MovingAverage(200)
        ema_50 = MovingAverage(50)
        ema_5 = MovingAverage(5)
        goo_emad_5_50 = GooEmaDelta(ema_short=5, ema_long=50, period=10)
        goo_emad_50_200 = GooEmaDelta(ema_short=50, ema_long=200, period=5)
               
        ema_200_attr = PlotAttribute(color='blue', linewidth=3)
        ema_50_attr = PlotAttribute(color='magenta', linewidth=3)
        ema_5_attr = PlotAttribute(color='yellow', linewidth=3)
        goo_emad_attr = PlotAttribute(color='green', linewidth=2, line_style=PlotAttribute.LineStyle.Histogram)

        chart = Chart(tags_spy).data(data_df)
        
        chart.add_sub_plot(ema_200, name="200 EMA", pane_index=0, plot_attribute=ema_200_attr)
        chart.add_sub_plot(ema_50, name="50 EMA", pane_index=0, plot_attribute=ema_50_attr)
        chart.add_sub_plot(ema_5, name="5 EMA", pane_index=0, plot_attribute=ema_5_attr)
        chart.add_sub_plot(goo_emad_5_50, name="GooEmaDelta 5-50", pane_index=1, plot_attribute=goo_emad_attr)
        chart.add_sub_plot(goo_emad_50_200, name="GooEmaDelta 50-200", pane_index=2, plot_attribute=goo_emad_attr)      
        chart.show()

    except Exception as e:
        print(f"Error fetching data: {e}")