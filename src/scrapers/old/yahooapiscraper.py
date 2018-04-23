#!/usr/bin/env python3

from datetime import datetime
from bs4 import BeautifulSoup
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import or_
from time import sleep
import dateutil.parser as dp
import urllib.request
import codecs
import re
import csv

from sa.logger import LOGGER
from sa.database import Session
from sa.models import Listings, YahooKeyStatistics

class YahooApiScraper():
    def __init__(self):
        # This is a reverse engineering of the Yahoo Finance REST API
        # Information off: http://www.jarloo.com/yahoo_finance/
        self.y_to_db_map = {'n': 'name', 'y': 'dividend_yield', 'd': 'dividend_ps',
                            'r': 'pe', 'r1': 'dividend_pay_date', 'q': 'ex_dividend_date',
                            'o': 'open', 'c1': 'change', 'p2': 'perc_change', 'd1': 'last_trade_date',
                            'd2': 'trade_date', 'c3': 'commission', 'g': 'day_low', 'h': 'day_high',
                            'p': 'previous_close', 't8': 'year_target', 'm5': 'change_mv_avg_200',
                            'm6': 'perc_change_mv_avg_200', 'm7': 'change_mv_avg_50', 'm8': 'perc_change_mv_avg_50',
                            'm3': 'mv_avg_50', 'm4': 'mv_avg_200', 'w1': 'day_value_change',
                            'g1': 'holding_gain_perc', 'g3': 'annualized_gain', 'g4': 'holdings_gain',
                            'k': 'high_52_week', 'j': 'low_52_week', 'j5': 'change_52_week_low',
                            'k4': 'change_52_week_high', 'j6': 'perc_change_52_week_low',
                            'k5': 'perc_change_52_week_high', 'j1': 'market_cap',
                            'f6': 'float_shares', 'x': 'stock_exchange', 's1': 'shares_owned',
                            'j2': 'shares_outstanding', 'n4': 'notes', 'i': 'more_info',
                            'v': 'volume', 'a2': 'avg_daily_volume', 'e': 'eps', 'e7': 'eps_year_estimate',
                            'e8': 'eps_next_year_estimate', 'e9': 'eps_next_q_estimate', 'b4': 'book',
                            'j4': 'ebitda', 'p5': 'price_sale', 'p6': 'price_book', 'r': 'pe', 'r5': 'peg',
                            'r6': 'price_eps_estimate_year', 'r7': 'price_eps_estimate_next_year', 's7': 'short_ratio',
                            's6': 'revenue', 'v1': 'holdings_val', 'l2': 'high_limit', 'l3': 'low_limit',
                            'a': 'ask', 'b': 'bid'}
        self.convert_dict = {'K': 10 ** 3, 'M': 10 ** 6, 'B': 10 ** 9, 'T': 10 ** 12}
        self.condensed_pat = re.compile("([+-]?\d*\.?\d+)([kmbtKMBT])")
        self.url_flags = tuple(self.y_to_db_map.keys())
        self.url_str_flags = "".join(self.url_flags)
        self.db_entries = tuple(self.y_to_db_map.values())
        self.float_pat = re.compile("[+-]?(\d*[\.])?\d+$")
        self.today = datetime.today().date()
        self.base_url = "http://finance.yahoo.com/d/quotes.csv"

        # Sometimes websites are friendlier to iOS devices :)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25"
        }
        self.session = Session()

    def handle_csv_string(self, string):
        if string == 'N/A':
            return None
        elif '%' in string:
            return float(string.strip('%'))
        elif string.isdigit():
            return int(string)
        elif self.condensed_pat.match(string) is not None:
            reg = self.condensed_pat.search(string)
            return int(float(reg.group(1)) * self.convert_dict[reg.group(2).upper()])
        elif self.float_pat.match(string) is not None:
            return float(string)
        else:
            try:
                return dp.parse(string).date().isoformat()
            except ValueError:
                return string

    def chunks(self, l, n):
        return [l[i:i + n] for i in range(0, len(l), n)]

    def create_url(self, tickers):
        ticker_str = "+".join(tickers)
        url = "{}?s={}&f={}".format(self.base_url, ticker_str, self.url_str_flags)
        return url

    def handle_url(self, tickers, url, exchange):
        """
        Fetches the url and inserts the data into the appropriate cols in the DB.
        """
        LOGGER.info("Starting to add url: {} ...".format(url))

        req =  urllib.request.Request(url, headers=self.headers)
        resp = urllib.request.urlopen(req)
        csv_r = csv.reader(codecs.iterdecode(resp, 'utf-8'))

        db_list = []
        for row, ticker in zip(csv_r, tickers):
            assert(len(row) == len(self.url_flags))

            db_dic = {db_col: self.handle_csv_string(cell) for cell, db_col in zip(row, self.db_entries)}

            onyahoo = any(v is not None for v in db_dic.values())

            self.session.query(Listings).filter(Listings.exchange == exchange,
                                                Listings.ticker == ticker
            ).update({Listings.onyahoo: onyahoo})

            if not onyahoo: # not found, skip
                LOGGER.error("Failed to find quote for {} skipping".format(ticker))
                continue

            db_dic["ticker"] = ticker
            db_dic["exchange"] = exchange

            exists = self.session.query(YahooKeyStatistics).filter_by(**db_dic).scalar() is not None
            if exists:
                LOGGER.info("Skipping {} due to prior existence".format(ticker))
                continue

            db_dic["update_date"] = self.today

            # Annoyingly enough, sqlalchemy doesn't allow PostgreSQL bulk inserts
            # when checking constraints, RIP performance
            stmt = insert(YahooKeyStatistics).values(db_dic).on_conflict_do_nothing(
                constraint = 'yahoo_key_statistics_pkey',
            )
            self.session.execute(stmt)
        self.session.commit()

        LOGGER.info("Done url.")

    def fetch_all(self, exchange):
        extension = '.TO'
        tickers = tuple(x.ticker + extension for x in self.session.query(Listings.ticker).filter(Listings.exchange == exchange, or_(Listings.onyahoo == True, Listings.onyahoo is None)))

        ticker_groups = self.chunks(tickers, 200)

        LOGGER.info("Fetching/Updating {} urls.".format(len(ticker_groups)))

        for ticker_group in ticker_groups:
            url = self.create_url(ticker_group)
            self.handle_url(ticker_group, url, exchange)
            sleep(1) # limit requests to 1/s

    def clean_exit(self):
        self.session.close()

if __name__ == "__main__":
    yas = YahooApiScraper()
    yas.fetch_all("TSX")
    yas.clean_exit()
