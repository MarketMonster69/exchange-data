#imports
from pybit import usdt_perpetual
import time, random
import pandas as pd
import os

#initial variables
session_unauth = usdt_perpetual.HTTP(endpoint="https://api.bybit.com")
symbols = ["BTCUSDT"]
intervals = ["M", "W", "D", "240", "60", "30", "15", "5", "1"]

#kline download path
default_data_dir = os.path.dirname(os.getcwd()) + "/datasets"
if not os.path.exists(default_data_dir):
    os.makedirs(default_data_dir)

def get_symbols():
    #gets all symbols/tickers using the session
    response = session_unauth.query_symbol()
    symbols = []
    for x in response["result"]:
        symbols.append(x["name"])
    return symbols

def get_sample_kline():
    #gets a small sample of kline data for testing
    response = session_unauth.query_kline(
        symbol = "BTCUSDT",
        interval = "1",
        from_time = 1,
        limit = 10
    )

    return response

def remove_last_row(f_path):
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

def update_last(f_path, symbol, interval):
    #deletes the last row in the dataset and replaces it with an updated row
    df = pd.read_csv(f_path, engine='python')
    last_row_csv = df.tail(1).values.tolist()
    startTime = last_row_csv[0][5]

    remove_last_row(f_path)

    kline_data = session_unauth.query_kline(
        symbol = symbol,
        interval = interval,
        from_time=startTime,
        limit=1
    )

    df_new = pd.DataFrame(kline_data["result"])
    df_new.to_csv(f_path, mode='a', index=False, header=False)
    return df_new

def kline_to_csv(symbols, intervals):
    #downloads bybit kline data to CSV files
    for symbol in symbols:
        for interval in intervals:

            f_name = symbol + "_" + interval + ".csv"
            f_path = os.path.join(default_data_dir, f_name)

            try:
                df_last = update_last(f_path, symbol, interval)
                startTime = df_last["open_time"].iat[-1] + 1
                print("replacing last row with updated...")
            except Exception as e:
                print(e)
                startTime = 1

            while True:

                kline_data = session_unauth.query_kline(
                    symbol = symbol,
                    interval = interval,
                    from_time=startTime
                )

                if kline_data["result"] is None:
                    print("no new data for " + f_name)
                    break

                print("...adding new data to CSV " + f_name)

                df = pd.DataFrame(kline_data["result"])
                df.to_csv(f_path, mode='a', index=False, header=False)

                startTime = df["open_time"].iat[-1] + 1
                
                time.sleep(random.uniform(3, 5))

    return

def kline_to_mongodb(symbols, intervals):
    return

kline_to_csv(symbols, intervals)

