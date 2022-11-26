#imports
from pybit import usdt_perpetual
import time, random
import pandas as pd


session_unauth = usdt_perpetual.HTTP(endpoint="https://api.bybit.com")


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
            df_kline = []

            while True:

                kline_data = session_unauth.query_kline(
                    symbol = symbol,
                    interval = interval,
                    startTime=startTime
                )

                if kline_data is None:
                    break

                df_kline = pd.concat(df_kline, pd.DataFrame(kline_data["result"]))
                startTime = df_kline["open_time"].iat[-1] + 1
                
                time.sleep(random.uniform(3, 5))

            df_kline.to_csv(symbol + interval + ".csv")

    return

def kline_to_mongodb(symbols, intervals):
    return

