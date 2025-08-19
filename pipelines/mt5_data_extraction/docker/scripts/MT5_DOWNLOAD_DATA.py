import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime, timedelta
import findspark
findspark.init()

from pyspark.sql import SparkSession
import duckdb
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MT5_Spread_Data") \
    .getOrCreate()

def fetch_symbol_data_with_spread(start_date, end_date, pairs, interval_days=30, output_dir="parquet_output"):
    # Initialize MetaTrader 5 platform
    if not mt5.initialize():
        print("Failed to initialize MetaTrader 5")
        return

    interval = timedelta(days=interval_days)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    for symbol, contract in pairs:
        all_data = []
        current_date = start_date

        while current_date < end_date:
            interval_end = min(current_date + interval, end_date)
            print(f"Fetching data for {symbol} from {current_date} to {interval_end}")

            # Get 1-minute candles
            rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_M1, current_date, interval_end)

            if rates is None or len(rates) == 0:
                print(f"No candle data for {symbol} between {current_date} and {interval_end}")
            else:
                # Get ticks to calculate spreads
                ticks = mt5.copy_ticks_range(symbol, current_date, interval_end, mt5.COPY_TICKS_ALL)
                tick_spreads = {}

                if ticks is not None and len(ticks) > 0:
                    for tick in ticks:
                        tick_time = datetime.fromtimestamp(tick['time']).strftime('%Y.%m.%d %H:%M')
                        tick_spreads.setdefault(tick_time, {'ask': [], 'bid': []})
                        tick_spreads[tick_time]['ask'].append(tick['ask'])
                        tick_spreads[tick_time]['bid'].append(tick['bid'])

                    # Average spread per minute
                    for tick_time, prices in tick_spreads.items():
                        avg_ask = sum(prices['ask']) / len(prices['ask'])
                        avg_bid = sum(prices['bid']) / len(prices['bid'])
                        tick_spreads[tick_time] = avg_ask - avg_bid

                # Combine candle and spread data
                for rate in rates:
                    time_str = datetime.fromtimestamp(rate['time']).strftime('%Y.%m.%d %H:%M:%S')
                    minute_key = datetime.fromtimestamp(rate['time']).strftime('%Y.%m.%d %H:%M')
                    spread = contract * tick_spreads.get(minute_key, rate['high'] - rate['low'])

                    all_data.append({
                        "SYMBOL": symbol,
                        "TIME": time_str,
                        "OPEN": rate['open'],
                        "HIGH": rate['high'],
                        "LOW": rate['low'],
                        "CLOSE": rate['close'],
                        "TICK_VOLUME": rate['tick_volume'],
                        "SPREAD": spread
                    })

            current_date = interval_end

        # Convert to Spark DataFrame
        df = pd.DataFrame(all_data)
        spark_df = spark.createDataFrame(df)

        # Save as Parquet file
        parquet_path = f"{output_dir}/{symbol.lower()}_spread_data.parquet"
        spark_df.write.mode("overwrite").parquet(parquet_path)
        print(f"✔ Spark wrote data for {symbol} to {parquet_path}")

        # Load into DuckDB as a table
        with duckdb.connect("mt5_data.duckdb") as con:
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS spread_data_{symbol.lower()} AS
                SELECT * FROM read_parquet('{parquet_path}')
            """)
            print(f"✔ DuckDB table 'spread_data_{symbol.lower()}' created from Parquet")

    # Shutdown MetaTrader
    mt5.shutdown()