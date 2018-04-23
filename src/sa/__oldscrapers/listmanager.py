#!/usr/bin/evn python3

import urllib.request
import dateutil.parser as dparser
import pandas as pd
from pandas_datareader.data import DataReader
from datetime import datetime, timedelta
import os

from sa.database import Database
from sa.common import create_url_request
from sa.logger import LOGGER


class ListManager():
    def __init__(self, cache=True, url="http://www.tsx.com/resource/en/571"):
        self.today = datetime.today().date()
        self.db = Database()
        self.url = url
        self.cache_path = os.path.join(os.getcwd(), "cache", "TSX", "listings") if cache else ""

    def write_cache(self, date, sheet):
        os.makedirs(self.cache_path, exist_ok=True)
        json_name = date.strftime('TSX-%Y-%m-%d.json')
        full_path = os.path.join(self.cache_path, json_name)
        sheet.to_json(full_path, orient="records")
        LOGGER.info("Wrote file to {}".format(full_path))
        
    def get_quotes(self):
        """
        This function gets the tickers and various other random information
        from the TSX website from a hardcoded file and inserts it into the database
        """
        recent_date = self.db.select("MAX(updatedate)", "listings", fetch="one", unroll=True)
        
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
            if self.cache_path:
                self.write_cache(recent_date, sheet)
        else:
            LOGGER.info("Already up to date")
            return

        row_names = ["updatedate", "ticker", "exchange", "name", "sector", "osshares",
                     "dateoflisting", "listingtype", "volume", "value",]

        all_excel_names = tuple(xlsx_dict[0].keys())
        base_wanted_excel_names = ["Root Ticker", "Exchange",
                                   "Name", "Sector", "O/S",
                                   "Date of TSX Listing", "Listing Type",
                                   "Volume YTD", "Value (C$)",]
        types = ["str", "str", "str", "str", "int",
                 "date", "str", "int", "int",]
        
        wanted_excel_names = []
        for bxn in base_wanted_excel_names:
            for xn in all_excel_names:
                if xn.startswith(bxn):
                    wanted_excel_names.append(xn)
                    break

        num_rows = len(wanted_excel_names)
        table_name = "listings"
        values = []
        for row in xlsx_dict:
            value_list = [recent_date]
            for i in range(num_rows):
                excel_name = wanted_excel_names[i]
                val = row[excel_name]
                if types[i] == "date":
                    val = datetime.strptime(str(val), "%Y%m%d") # assume YYYYMMDD
                value_list.append(val)
            values.append(value_list)

        self.db.insert_into(table_name, row_names, values)

    def get_historic_events(self):
        """
        Gets all the historical events from yahoo, updating only the new entries
        based on the date of the last fetch.
        """
        exchange = "TSX"
        listings = self.db.select("ticker, dateoflisting", "listings", where="exchange = %s", vals=[exchange])
        dict_fields = ["index", "action", "value"]
        fields = ["exchange", "ticker", "date", "action", "value"]

        total_listings = len(listings)        
        for counter, (ticker, listdate) in enumerate(listings):
            lastdate = self.db.select("MAX(date)", "event_history", fetch="one", where="exchange = %s AND ticker = %s",
                                      vals=[exchange, ticker], unroll=True)
            startdate = listdate if lastdate is None else lastdate + timedelta(days=1)

            event_dict = []
            if startdate < self.today:
                yahoo_ticker = ticker + ".TO"
                event_dict = self.ticker_event_history(startdate, self.today, yahoo_ticker)

            if event_dict:
                LOGGER.info("{}/{} Inserting {} from {} to {}".format(counter, total_listings, ticker, startdate, self.today))
                rows = [[exchange, ticker] + [row[k] for k in dict_fields] for row in event_dict]
                self.db.insert_into("event_history", fields, rows)
            else:
                LOGGER.info("{}/{} Skipping ticker {}".format(counter, total_listings, ticker))
        
    def get_historic_prices(self):
        """
        Gets all the historical prices from yahoo, updating only the new entries
        based on the date of the last fetch.
        """
        
        exchange = "TSX"
        listings = self.db.select("ticker, dateoflisting", "listings", where="exchange = %s", vals=[exchange])
        dict_fields = ["Adj Close", "High", "Close", "Open", "Low", "Date"]
        fields = ["exchange", "ticker"] + [x.lower() for x in dict_fields]        

        total_listings = len(listings)        
        for counter, (ticker, listdate) in enumerate(listings):
            lastdate = self.db.select("MAX(date)", "price_history", fetch="one", where="exchange = %s AND ticker = %s",
                                      vals=[exchange, ticker], unroll=True)
            startdate = listdate if lastdate is None else lastdate + timedelta(days=1)

            his_dict = []
            if startdate < self.today:
                yahoo_ticker = ticker + ".TO"
                his_dict = self.ticker_price_history(startdate, self.today, yahoo_ticker)

            if his_dict:
                LOGGER.info("{}/{} Inserting {} from {} to {}".format(counter, total_listings, ticker, startdate, self.today))
                rows = [[exchange, ticker] + [row[k] for k in dict_fields] for row in his_dict]
                self.db.insert_into("price_history", fields, rows)
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

    def ticker_price_history(self, start, end, ticker):
        """
        Gets and returns the historic prices for a given ticker for between
        the time period provided. Inclusive.
        """
        try:
            dr = DataReader(ticker, 'yahoo', start, end).reset_index()
        except OSError:
            return []  # if the page cannot be reached for some reason

        return dr.to_dict(orient="records")

    def ticker_event_history(self, start, end, ticker):
       try:
            dr = DataReader(ticker, 'yahoo-actions', start, end).reset_index()
       except OSError:
           return []  # if the page cannot be reached for some reason

       return dr.to_dict(orient="records")

    def clean_exit(self):
        self.db.destroy()


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
