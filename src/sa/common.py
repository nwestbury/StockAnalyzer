#!/usr/bin/env python3

import urllib.request
from sqlalchemy import distinct
from sqlalchemy.sql import func
from sa.models import Listings, MorningStarKeyStatistics, KeyStatistics


def get_ticker_names(sess, exchange):
    return tuple(ticker for ticker, in sess.query(distinct(Listings.ticker)).filter(Listings.exchange == exchange))

def get_ms_ticker_names(sess, exchange):
    return tuple(ticker for ticker, in sess.query(distinct(MorningStarKeyStatistics.ticker)).filter(MorningStarKeyStatistics.exchange == exchange))

def is_ticker(sess, exchange, ticker):
    query = sess.query(Listings).filter(Listings.exchange == exchange, Listings.ticker == ticker)
    return sess.query(query.exists())

def find_small_cap_tickers(sess, value=10*10**6):
    latest_date, = sess.query(func.max(KeyStatistics.update_date)).first()
    q = sess.query(KeyStatistics.ticker).filter(
        KeyStatistics.update_date == latest_date,
        KeyStatistics.market_cap < value
    )

    rows = sess.execute(q).fetchall()

    return [ticker for ticker, in rows]


def create_url_request(url):
    return urllib.request.Request(
        url,
        headers={
            "User-Agent": ("Mozilla/5.0 (Windows NT 6.1; WOW64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/43.0.2357.134 Safari/537.36")
        }
    )
