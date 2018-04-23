#!/usr/bin/evn python3

import urllib.request
import dateutil.parser as dparser
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func
from yahoo_quote_download import yqd
from socket import gaierror
from urllib.error import HTTPError, URLError

from sa.database import Session
from sa.models import Listings, PriceHistory, EventHistory
from sa.common import create_url_request
from sa.logger import LOGGER


class ListManager():
    def __init__(self, url="http://www.tsx.com/resource/en/571"):
        self.today = datetime.today().date()
        self.session = Session()
        self.url = url

    def get_quotes(self):
        """
        This function gets the tickers and various other random information
        from the TSX website from a hardcoded file and inserts it into the database
        """
        recent_date, = self.session.query(func.max(Listings.updatedate)).first()

        if self.url.startswith("http"):
            req = create_url_request(self.url)
            self.url = urllib.request.urlopen(req)
        
        sheet = pd.read_excel(self.url, skiprows=5, header=1, keep_default_na=False)
        sheet.fillna('', inplace=True)
        sheet.rename(columns=self.cleanse_str, inplace=True)
        
        file_date = self.find_date_in_list(list(sheet.columns.values))

        if recent_date is None or (file_date > recent_date):
            xlsx_dict = sheet.to_dict(orient="records")
            recent_date = file_date
        else:
            LOGGER.info("Already up to date")
            return

        row_names = ["ticker", "exchange", "name", "sector", "osshares",
                     "dateoflisting", "listingtype", "volume", "value",]

        all_excel_names = tuple(xlsx_dict[0].keys())
        base_wanted_excel_names = ["Root Ticker", "Exchange",
                                   "Name", "Sector", "O/S",
                                   "Date of TSX Listing", "Listing Type",
                                   "Volume YTD", "Value (C$)",]
        wanted_excel_names = []
        for bxn in base_wanted_excel_names:
            for xn in all_excel_names:
                if xn.startswith(bxn):
                    wanted_excel_names.append(xn)
                    break

        assert(len(base_wanted_excel_names) == len(wanted_excel_names) == len(row_names))

        value_dics = []
        for row in xlsx_dict:
            value_dic = {"updatedate": recent_date}
            for excel_name, row_name in zip(wanted_excel_names, row_names):
                val = row[excel_name]
                if row_name == "dateoflisting":
                    val = datetime.strptime(str(val), "%Y%m%d") # assume YYYYMMDD
                if val == '':
                    val = None
                value_dic[row_name] = val
            value_dics.append(value_dic)

        self.session.execute(insert(Listings).values(value_dics))
        self.session.commit()

    def get_historic_events(self):
        """
        Gets all the historical events from yahoo, updating only the new entries
        based on the date of the last fetch.
        """
        exchange = "TSX"
        listings = self.session.query(Listings.ticker, Listings.dateoflisting).filter(
            Listings.exchange == exchange
        )

        dict_fields = ["index", "action", "value"]
        fields = ["exchange", "ticker", "date", "action", "value"]
        total_listings = listings.count()

        for counter, (ticker, listdate) in enumerate(listings):
            lastdate, = self.session.query(func.max(EventHistory.updatedate)).filter(
                EventHistory.exchange == exchange,
                EventHistory.ticker   == ticker
            ).first()

            startdate = listdate if lastdate is None else lastdate + timedelta(days=1)

            rows = []
            if startdate < self.today:
                yahoo_ticker = ticker + ".TO"

                dividend_dict = self.ticker_history(startdate, self.today, yahoo_ticker, info='dividend')
                split_dict = self.ticker_history(startdate, self.today, yahoo_ticker, info='split')
                rows = []
                for row in dividend_dict:
                    rows.append([exchange, ticker, row["date"], "DIVIDEND", row["dividends"], self.today])
                for row in split_dict:
                    rows.append([exchange, ticker, row["date"], "SPLIT", row["stock_splits"], self.today])

            if rows:
                LOGGER.info("{}/{} Inserting {} from {} to {}".format(counter+1, total_listings, ticker, startdate, self.today))
                stmt = insert(EventHistory).values(rows).on_conflict_do_nothing(
                    constraint = 'event_history_pkey'
                )
                self.session.execute(stmt)
                self.session.commit()
            else:
                LOGGER.info("{}/{} Skipping ticker {}".format(counter+1, total_listings, ticker))

    def get_historic_prices(self):
        """
        Gets all the historical prices from yahoo, updating only the new entries
        based on the date of the last fetch.
        """
        
        exchange = "TSX"

        listings = list(self.session.query(Listings.ticker, Listings.dateoflisting).filter(
            Listings.exchange == exchange
        ))
        total_listings = len(listings)

        for counter, (ticker, listdate) in enumerate(listings):
            lastdate, = self.session.query(func.max(PriceHistory.date)).filter(
                PriceHistory.exchange == exchange,
                PriceHistory.ticker   == ticker
            ).first()

            startdate = listdate if lastdate is None else lastdate + timedelta(days=1)

            his_dict = []
            if startdate < self.today:
                yahoo_ticker = ticker + ".TO"
                start_dic = {"exchange": exchange, "ticker": ticker}
                his_dict = self.ticker_history(startdate, self.today, yahoo_ticker, info="quote", start_dic=start_dic)

            if his_dict:
                LOGGER.info("{}/{} Inserting {} from {} to {}".format(counter, total_listings, ticker, startdate, self.today))

                for d in his_dict:
                    stmt = insert(PriceHistory).values(d).on_conflict_do_update(
                        constraint='price_history_pkey',
                        set_=d
                    )
                    self.session.execute(stmt)

                self.session.commit()
            else:
                LOGGER.info("{}/{} Skipping ticker {}".format(counter, total_listings, ticker))

    def cleanse_str(self, raw_str):
        return raw_str.replace('\n', ' ').replace("  ", ' ')

    def find_date_in_list(self, strings):
        """
        Returns the first date that occurs in a list of string
        or the current date if none are detected.
        """
        cur_date = self.today  # default = cur. date
        for s in strings:
            try:
                temp_date = dparser.parse(s, fuzzy=True).date()
            except ValueError:
                continue
            
            if cur_date != temp_date:
                cur_date = temp_date
                break
        return cur_date

    def convert_yahoo_element(self, element):
        converted = None
        try:
            converted = float(element)
        except ValueError:
            try:
                converted = datetime.strptime(element, "%Y-%m-%d")
            except ValueError:
                if element == 'null':
                    converted = None
                elif '/' in element:
                    try:
                        a, b = element.split('/')
                        converted = float(a) / float(b)
                    except ValueError:
                        LOGGER.info("Unable to convert {}".format(element))
                else:
                    LOGGER.info("Unable to convert {}".format(element))

        return converted

    def ticker_history(self, start, end, ticker, info='quote', start_dic={}):
        """
        Gets and returns the historic prices for a given ticker for between
        the time period provided. Inclusive.
        """

        start_str = start.strftime('%Y%m%d')
        end_str   = end.strftime('%Y%m%d')

        # info = 'quote', 'dividend', 'split'
        try:
            data =  yqd.load_yahoo_quote(ticker, start_str, end_str, info=info)
        except (HTTPError, URLError, gaierror) as e:
            LOGGER.info("Yahoo request failed. Blocked?")
            return []

        titles = tuple(t.replace(' ', '_').lower() for t in data[0].split(','))

        history = []
        for row in data[1:-1]:
            history_row = {k: v for k, v in start_dic.items()}
            iter_list = row.split(',')

            for element, title in zip(iter_list, titles):
                converted = self.convert_yahoo_element(element)
                history_row[title] = converted
            history.append(history_row)
        return history

    def clean_exit(self):
        self.session.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: {} [-h, --history] [-q, --quotes] [-e, --events]".format(sys.argv[0]))
        sys.exit(1)

    lm = ListManager()

    if sys.argv[1] == '-h' or sys.argv[1] == '--history':
        lm.get_historic_prices()
    elif sys.argv[1] == '-q' or sys.argv[1] == '--quotes':
        lm.get_quotes()
    elif sys.argv[1] == '-e' or sys.argv[1] == '--events':
        lm.get_historic_events()
    else:
        print("Unrecognized argument", sys.argv[1])
        sys.exit(1)

    lm.clean_exit()
