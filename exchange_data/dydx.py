#!/usr/bin/env python

from dydx3 import Client
import dydx3.constants as consts
import pandas as pd

class BarDownloader:

    def __init__(self):
        self.client = Client(
            host='https://api.dydx.exchange'
        )
        pass

    def get_candles(self):
        market = consts.MARKET_BTC_USD
        candles = self.client.public.get_candles(
        market=market,
        resolution='1DAY',
        from_iso="2022-01-01T00:00:00.000Z",
        limit=100,
        )

        df = pd.DataFrame(candles.data['candles'])
        return df

if __name__ == "__main__":
    bd = BarDownloader()
    print(bd.get_candles())