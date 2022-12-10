#!/usr/bin/env python

import os
import time
import random
import pandas as pd
from pybit import usdt_perpetual

class KlineDownloader:
    def __init__(self, endpoint="https://api.bybit.com"):
        self.session = usdt_perpetual.HTTP(endpoint=endpoint)
        self.symbols = ["BTCUSDT"]
        self.intervals = ["M", "W", "D", "240", "60", "30", "15", "5", "1"]

    def get_symbols(self):
        response = self.session.query_symbol()
        symbols = []
        for x in response["result"]:
            symbols.append(x["name"])
        return symbols

    def remove_last_row(self, f_path):
        #removes last row in a CSV file using a pointer - better for large files
        #https://stackoverflow.com/questions/1877999/delete-final-line-in-file-with-python/10289740

        with open(f_path, "r+", encoding = "utf-8") as file:
            # Move the pointer (similar to a cursor in a text editor) to the end of the file
            file.seek(0, os.SEEK_END)

            # This code means the following code skips the very last character in the file -
            # i.e. in the case the last line is null we delete the last line
            # and the penultimate one
            pos = file.tell() - 1

            # Read each character in the file one at a time from the penultimate
            # character going backwards, searching for a newline character
            # If we find a new line, exit the search
            while pos > 0 and file.read(1) != "\n":
                pos -= 1
                file.seek(pos, os.SEEK_SET)

            # So long as we're not at the start of the file, delete all the characters ahead
            # of this position
            if pos > 0:
                file.seek(pos + 1, os.SEEK_SET)
                file.truncate()

        return

    def update_last(self, f_path, symbol, interval):
        #deletes the last row in the dataset and replaces it with an updated row
        df = pd.read_csv(f_path, engine='python', header=0)
        last_row_csv = df.tail(1).values.tolist()
        start_time = last_row_csv[0][5]

        # Query the API for the updated data
        kline_data = self.session.query_kline(
            symbol=symbol, interval=interval, from_time=start_time, limit=1
        )

        # Update the dataframe and save it to the file
        df_new = pd.DataFrame(kline_data["result"])

        self.remove_last_row(f_path)

        # Save only the updated row to the CSV file
        df_new.to_csv(f_path, mode="a", index=False, header=False)

        return df_new


    def kline_to_csv(self, symbols, intervals):
        # Set the default data directory
        default_data_dir = os.path.dirname(os.getcwd()) + "/datasets"
        if not os.path.exists(default_data_dir):
            os.makedirs(default_data_dir)

        # Download the data and save it to CSV files
        for symbol in symbols:
            for interval in intervals:
                f_name = symbol + "_" + interval + ".csv"
                f_path = os.path.join(default_data_dir, f_name)

                try:
                    df_last = self.update_last(f_path, symbol, interval)
                    start_time = df_last["open_time"] + 1
                    df_header=False
                except FileNotFoundError:
                    print("file does not exist")
                    df_header=True
                    start_time = 1

                while True:
                    # Query the API for the next chunk of data
                    kline_data = self.session.query_kline(
                        symbol = symbol,
                        interval = interval,
                        from_time=start_time
                    )

                    if kline_data["result"] is None:
                        break

                    # Append the data to the file
                    df = pd.DataFrame(kline_data["result"])
                    df.to_csv(f_path, mode="a", index=False, header=df_header)

                    start_time = df["open_time"].iat[-1] + 1

                    # Sleep for a random interval to avoid hitting rate limits
                    time.sleep(random.uniform(0.5, 1.0))


if __name__ == "__main__":
    kd = KlineDownloader()
    kd.kline_to_csv(kd.symbols, kd.intervals)