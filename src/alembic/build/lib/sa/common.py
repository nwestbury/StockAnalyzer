#!/usr/bin/env python3

import urllib.request
     
def get_ticker_names(db, exchange):
    return db.select("ticker", "listings", where="exchange = %s", vals=[exchange], unroll=True)

def is_ticker(db, exchange, ticker):
    return db.exists("listings", "exchange=%s AND ticker=%s", [exchange, ticker])
    
def create_url_request(url):
    return urllib.request.Request(
        url,
        headers={
            "User-Agent": ("Mozilla/5.0 (Windows NT 6.1; WOW64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/43.0.2357.134 Safari/537.36")
        }
    )
