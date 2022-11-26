#imports
from pybit import usdt_perpetual
import time, random
import pandas as pd
import os


session_unauth = usdt_perpetual.HTTP(endpoint="https://api.bybit.com")
symbols = ["BTCUSDT"]
intervals = ["M", "W", "D", "240", "60", "30"]

#kline download path
default_data_dir = os.path.dirname(os.getcwd()) + "/datasets"
if not os.path.exists(default_data_dir):
    os.makedirs(default_data_dir)

def get_symbols():

    response = session_unauth.query_symbol()
    symbols = []
    for x in response["result"]:
        symbols.append(x["name"])
    return symbols

def get_sample_kline():

    #query data
    response = session_unauth.query_kline(
        symbol = "BTCUSDT",
        interval = "1",
        from_time = 1,
        limit = 10
    )

    return response

def kline_to_csv(symbols, intervals):
    for symbol in symbols:
        for interval in intervals:

            startTime = 1
            f_name = symbol + "_" + interval + ".csv"
            f_path = os.path.join(default_data_dir, f_name)

            while True:

                kline_data = session_unauth.query_kline(
                    symbol = symbol,
                    interval = interval,
                    from_time=startTime
                )

                if kline_data["result"] is None:
                    break

                df = pd.DataFrame(kline_data["result"])
                df.to_csv(f_path, mode='a', index=False, header=False)

                startTime = df["open_time"].iat[-1] + 1
                
                time.sleep(random.uniform(3, 5))

    return

def kline_to_mongodb(symbols, intervals):
    return

kline_to_csv(symbols, intervals)

