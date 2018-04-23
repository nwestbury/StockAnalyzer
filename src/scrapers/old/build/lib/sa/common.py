#!/usr/bin/env python3

import urllib.request
from sqlalchemy import distinct
from sa.models import Listings, MorningStarKeyStatistics


def get_ticker_names(sess, exchange):
    return tuple(ticker for ticker, in sess.query(distinct(Listings.ticker)).filter(Listings.exchange == exchange))

def get_ms_ticker_names(sess, exchange):
    return tuple(ticker for ticker, in sess.query(distinct(MorningStarKeyStatistics.ticker)).filter(MorningStarKeyStatistics.exchange == exchange))

def is_ticker(sess, exchange, ticker):
    query = sess.query(Listings).filter(Listings.exchange == exchange, Listings.ticker == ticker)
    return sess.query(query.exists())


def create_url_request(url):
    return urllib.request.Request(
        url,
        headers={
            "User-Agent": ("Mozilla/5.0 (Windows NT 6.1; WOW64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/43.0.2357.134 Safari/537.36")
        }
    )
