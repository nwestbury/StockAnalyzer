#!/usr/bin/env python3

from db import db_connect


def get_ticker_names():
    client = db_connect()
    collection = client["stockAnalyzer"]["TSX-History"]
    tickers_cursor = collection.find({}, {"Root Ticker": True})

    return [ticker["Root Ticker"] for ticker in tickers_cursor]


def get_latest_summary_stats(ticker=None, limit=0):
    client = db_connect()
    collection = client["stockAnalyzer"]["TSX-Stock"]
    option_dict = {"_id": False, "Date": False, "Root Ticker": False}

    if ticker is None:
        sum_cursor = collection.find({}, {"_id": False}, limit=limit)
        sum_list = {k: i[-1]["Value"]
                    for row in sum_cursor for k, i in row.items()}
    else:
        base_dict = collection.find_one({"Root Ticker": ticker}, option_dict)
        sum_list = {title: valList[-1]["Value"]
                    for title, valList in base_dict.items()}
    return sum_list


def get_historical_list(ticker=None, limit=0):
    client = db_connect()
    collection = client["stockAnalyzer"]["TSX-History"]

    if ticker is None:
        his_cursor = collection.find({}, {"History": True, "_id": False},
                                     limit=limit)
        his_list = [row["History"] for row in his_cursor]
    else:
        his_list = collection.find_one({"Root Ticker": ticker},
                                       {"History": True,
                                        "_id": False})["History"]

    return his_list

if __name__ == "__main__":
    get_latest_summary_stats(ticker="AAB.TO")
