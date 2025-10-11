from datasources import PolygonIO, DataSourceHelpers
from technical_analysis import MovingAverage
from influx_database import InfluxDatabase, InfluxQuery
from tags import InstrumentTags
from charts import Chart


if __name__ == "__main__":   
    try:
        ticker = "SPY"
        #ds_polygon = PolygonIO()
        #data = ds_polygon.download_data(ticker, "2022-01-01")
        #DataSourceHelpers.display_ohlc(data,ticker)
        
        db = InfluxDatabase()
        tags = InstrumentTags(symbol=ticker)
        #db.write_pandas(dataframe=data,tags=tags)

        tags = InstrumentTags(symbol="spy")
        query = InfluxQuery().range().add_tag_group(tags).build(db)
        data_df = db.read_records(query)
        ema_200 = MovingAverage(200).calculate(data_df)
        chart = Chart("SPY Stock Chart").data(data_df)
        chart.show()



    except Exception as e:
        print(f"Error fetching data: {e}")