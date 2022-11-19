#imports
from pybit import usdt_perpetual

#handle session(s)
session_unauth = usdt_perpetual.HTTP(endpoint="https://api.bybit.com")

def get_symbols(session):

    response = session.query_symbol()
    symbols = []
    for x in response["result"]:
        symbols.append(x["name"])
    return symbols

def get_sample_kline(session):

    #query data
    response = session.query_kline(
        symbol = "BTCUSDT",
        interval = "1",
        from_time = 1,
        limit = 10
    )

    return response


