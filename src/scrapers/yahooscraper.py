#!/usr/bin/env python3

from datetime import datetime
from bs4 import BeautifulSoup
from jspagescraper import JSPageScraper
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import or_
import dateutil.parser as dp
import re

from sa.logger import LOGGER
from sa.database import Session
from sa.models import Listings, KeyStatistics

class YahooScraper():
    def __init__(self):
        self.y_to_db_map = {'Forward P/E': 'forward_pe', 'Return on Equity': 'ro_equity', 'Current Ratio': 'current_ratio', 'Total Debt': 'total_debt', 'Forward Annual Dividend Rate': 'forward_annual_dividend_rate', 'Last Split Date': 'last_split_date', 'Market Cap (intraday)': 'market_cap', 'EBITDA': 'ebitda', 'Shares Short': 'shares_short', '50-Day Moving Average': 'fifty_day_moving_avg', '52 Week High': 'fifty_two_week_high', 'Quarterly Earnings Growth': 'q_earnings_growth', 'Forward Annual Dividend Yield': 'forward_annual_dividend_yield', 'Beta': 'beta', 'Payout Ratio': 'payout_ratio', 'Avg Vol (3 month)': 'avg_vol_3_month', 'Enterprise Value': 'enterprise_value', '5 Year Average Dividend Yield': 'five_year_avg_dividend_yield', 'Enterprise Value/Revenue': 'enterprise_value_revenue', 'Trailing P/E': 'trailing_pe', 'Total Cash': 'total_cash', 'Operating Cash Flow': 'operating_cash_flow', 'Price/Book': 'price_book', 'Fiscal Year Ends': 'fiscal_year_ends', 'Total Debt/Equity': 'total_debt_equity', 'Dividend Date': 'dividend_date', 'Most Recent Quarter': 'most_recent_q', 'Operating Margin': 'operating_margin', 'Ex-Dividend Date': 'exdividend_date', '% Held by Institutions': 'perc_held_by_institutions', 'Trailing Annual Dividend Yield': 'trailing_annual_dividend_yield', '200-Day Moving Average': 'two_hundred_day_moving_avg', '52 Week Low': 'fifty_two_week_low', 'Avg Vol (10 day)': 'avg_vol_10_day', 'Last Split Factor (new per old)': 'last_split_factor', '% Held by Insiders': 'perc_held_by_insiders', 'Revenue Per Share': 'revenue_per_share', 'Short Ratio': 'short_ratio', 'Shares Short (prior month)': 'shares_short_prior_month', 'Short % of Float': 'short_perc_float', 'Profit Margin': 'profit_margin', 'Return on Assets': 'ro_assets', 'Price/Sales': 'price_sales', 'Gross Profit': 'gross_profit', 'Book Value Per Share': 'book_value_per_share', 'Levered Free Cash Flow': 'levered_free_cash_flow', 'Trailing Annual Dividend Rate': 'trailing_annual_dividend_rate', 'Diluted EPS': 'diluted_eps', 'PEG Ratio (5 yr expected)': 'peg_ratio_5yr', 'Shares Outstanding': 'shares_outstanding', 'Revenue': 'revenue', 'Float': 'float', 'Net Income Avi to Common': 'net_income_avi_common', 'Enterprise Value/EBITDA': 'enterprise_value_ebitda', '52-Week Change': 'fifty_two_week_change', 'Quarterly Revenue Growth': 'q_revenue_growth', 'Total Cash Per Share': 'total_cash_ps'}
        self.convert_dict = {'K': 10 ** 3, 'M': 10 ** 6, 'B': 10 ** 9,
                             'T': 10 ** 12}
        self.condensed_pat = re.compile("([+-]?\d*[\.]?\d+)([kmbtKMBT])$")
        self.float_pat = re.compile("[+-]?\d*[\.]\d+$")
        self.parenthese_pat = re.compile(" *\(([^)]*)\)")
        self.date_line_pat = re.compile("\(as of (\d+.*\d+)\)")
        self.url_ticker_pat = re.compile(".*/quote/(.*)\.(.*)/key-statistics")
        self.keywords = set({"mrq", "ttm", "yoy", "lfy", "fye"})
        self.today = datetime.today().date()
        self.default_fye = datetime(self.today.year, 12, 31)
        self.session = Session()

    def s2n(self, string):
        reg = self.condensed_pat.search(string)
        return int(float(reg.group(1)) * self.convert_dict[reg.group(2).upper()])

    @staticmethod
    def s2p(string):
        return float(string.strip('%'))

    @staticmethod
    def s2r(string):
        split = string.split(':')
        return float(split[0]) / float(split[1])

    def parse_numeric(self, string):
        try:
            if '%' in string:
                return self.s2p(string)
            elif self.condensed_pat.match(string) is not None:
                return self.s2n(string)
            elif self.float_pat.match(string) is not None:
                return float(string)
            elif string.isdigit():
                return int(string)
            elif ':' in string:
                return self.s2r(string)
            else:
                return dp.parse(string).date().isoformat()
        except ValueError:
            return None

    def cleanse_str(self, string):
        return self.parenthese_pat.sub('', string.replace(',', '')).strip(':')

    def dic_parse(self, session, url, html):
        def innerHtml(ele):
            return ele.decode_contents(formatter="html")
        
        soup = BeautifulSoup(html, "lxml")
        ticker = self.url_ticker_pat.search(url).group(1)
        exchange = "TSX"

        on_yahoo = soup.find('section', attrs={'data-test': 'lookup-page'}) is None
        session.query(Listings).filter(Listings.exchange == exchange,
                                            Listings.ticker == ticker
        ).update({Listings.onyahoo: on_yahoo})

        if not on_yahoo: # if quote not found, exit
            LOGGER.error("Failed to find quote for {} skipping".format(url))
            return
        
        div_test = soup.find('section', attrs={'data-test' : 'qsp-statistics'})
        if div_test is None:
            LOGGER.error("Unknown error for {} skipping".format(url))
            return
        
        db_dic = {}
        for table in div_test.find_all('table'):
            for row in table.find_all('tr'): 
                td_list = row.find_all('td')
                title = innerHtml(td_list[0].find('span'))
                val = innerHtml(td_list[1]) if td_list[1].find('span') is None else innerHtml(td_list[1].find('span'))
                if title in self.y_to_db_map:
                    db_dic[self.y_to_db_map[title]] = self.parse_numeric(val)

        if db_dic:
            db_dic["ticker"] = ticker
            db_dic["exchange"] = exchange
            exists = session.query(KeyStatistics).filter_by(**db_dic).scalar() is not None

            if exists:
                LOGGER.info("Skipping {} due to prior existence".format(url))
            else:
                db_dic["update_date"] = self.today

                stmt = insert(KeyStatistics).values(db_dic).on_conflict_do_nothing(
                    constraint = 'key_statistics_pkey',
                )
                session.execute(stmt)
                session.commit()

                LOGGER.info("Done parsing {}".format(url))
        else:
            LOGGER.info("Skipping {}".format(url))

    def fetch_all(self, exchange):
        q = self.session.query(Listings).filter(Listings.exchange == exchange, or_(Listings.onyahoo == True, Listings.onyahoo is None))

        extension = '.TO'
        urls = ["https://ca.finance.yahoo.com/quote/{}{}/key-statistics".
                format(l.ticker, extension) for l in q]

        xpath_hooks = ["//section[@data-test='qsp-statistics']", "//section[@data-test='lookup-page']"]

        LOGGER.info("Fetching/Updating {} urls.".format(len(urls)))

        jsps = JSPageScraper(self.dic_parse, xpath_hooks, "key_statistics")
        jsps.go(urls)

    def clean_exit(self):
        self.session.close()

if __name__ == "__main__":
    ys = YahooScraper()
    ys.fetch_all("TSX")
    ys.clean_exit()
