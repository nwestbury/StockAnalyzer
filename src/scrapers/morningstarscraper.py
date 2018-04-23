import os
from datetime import datetime
from random import randint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_, or_
import codecs
import urllib.request
import csv

from sa.database import Session
from sa.logger import LOGGER
from sa.models import Listings, MorningStarKeyStatistics, MorningStarFinancials

def get_module_dir():
    return os.path.dirname(os.path.realpath(__file__))

class MorningStarScaper():
    def __init__(self):
        self.session = Session()
        self.today = datetime.today().date()
        self.ttm_string = self.most_recent_quarter()
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36"
        }
        self.exchange_map = {
            "XTSE": "TSX",
        }

        self.year_month_cols = set({
            "fiscal_year", "margin_date", "profitability_date"
        })
        
        self.column_key_map = tuple((
            ("revenue", "revenue"),
            ("gross margin", "gross_margin"),
            ("operating income", "operating_income"),
            ("operating margin", "operating_margin"),
            ("net income", "net_income"),
            ("earnings per share", "eps"),
            ("dividends", "dividends"),
            ("payout ratio", "payout_ratio"),
            ("shares", "num_shares"),
            ("book value per", "book_value_ps"),
            ("operating cash flow", "operating_cash_flow"),
            ("cap spending", "cap_spending"),
            ("cf free cash flow growth", "free_cash_flow_growth_yoy"),
            ("cf free cash flow/sales", "free_cash_flow_sales"),
            ("cf free cash flow/net", "free_cash_flow_net_income"),
            ("free cash flow per share", "free_cash_flow_ps"),
            ("free cash flow", "free_cash_flow"),
            ("working capital", "working_captial"),
            ("pro margins %", "margin_date"),
            ("pro revenue", "revenue_per_sales"),
            ("pro cogs", "revenue_per_cogs"),
            ("pro gross margin", "sales_gross_margin"),
            ("pro sg&a", "margin_sga"),
            ("pro r&d", "margin_rd"),
            ("pro other", "margin_other"),
            ("pro operating margin", "margin_operating"),
            ("pro net int inc", "margin_net_income"),
            ("pro ebt margin", "margin_ebt"),
            ("pro profitability", "profitability_date"),
            ("pro tax rate", "tax_rate"),
            ("pro net margin", "net_margin_perc"),
            ("pro asset turnover", "asset_turnover"),
            ("pro return on assets", "ro_assets"),
            ("pro financial lever", "financial_leverage"),
            ("pro return on equity", "ro_equity"),
            ("pro return on invested capital", "ro_invested_captial"),
            ("pro interest coverage", "interest_coverage"),
            ("r% year over year", "revenue_perc_yoy"),
            ("r% 3-year", "revenue_perc_3y"),
            ("r% 5-year", "revenue_perc_5y"),
            ("r% 10-year", "revenue_perc_10y"),
            ("oi% year over year", "operating_income_yoy"),
            ("oi% 3-year", "operating_income_3y"),
            ("oi% 5-year", "operating_income_5y"),
            ("oi% 10-year", "operating_income_10y"),
            ("ni% year over year", "net_income_yoy"),
            ("ni% 3-year", "net_income_3y"),
            ("ni% 5-year", "net_income_5y"),
            ("ni% 10-year", "net_income_10y"),
            ("eps% year over year", "eps_yoy"),
            ("eps% 3-year", "eps_3y"),
            ("eps% 5-year", "eps_5y"),
            ("eps% 10-year", "eps_10y"),
            ("cf operating cash flow", "cash_flow_operating_growth_yoy"),
            ("cf cap ex", "cap_expense_perc_sales"),
            ("fh cash & short", "cash_short_term"),
            ("fh accounts receivable", "accounts_receivable"),
            ("fh inventory", "inventory"),
            ("fh other current assets", "other_cur_assets"),
            ("fh total current assets", "total_cur_assets"),
            ("fh net pp&e", "net_ppe"),
            ("fh intangibles", "intangibles"),
            ("fh other long-term assets", "other_long_term_assets"),
            ("fh accounts payable", "accounts_payable"),
            ("fh short-term debt", "short_term_debt"),
            ("fh taxes payable", "taxes_payable"),
            ("fh accured liabilities", "accured_liabilities"),
            ("fh other short-term liabilities", "short_term_liabilities"),
            ("fh long-term debt", "long_term_debt"),
            ("fh total liabilities & equity", "total_liabilities_equity"),
            ("fh total liabilities", "total_liabilities"),
            ("fh total stockholder", "total_stockholder"),
            ("fh current ratio", "current_ratio"),
            ("fh quick ratio", "quick_ratio"),
            ("fh debt/equity", "debt_equity"),
            ("er receivables turnover", "receivables_turnover"),
            ("er inventory turnover", "inventory_turnover"),
            ("er fixed assets turnover", "fixed_assets_turnover"),
        ))
        self.column_financials_map = tuple((
            ("fiscal year", "fiscal_year"),
            ("revenue", "revenue"),
            ("cost of revenue", "revenue_cost"),
            ("gross profit", "gross_profit"),
            ("sales, general and administrative", "sales_expense"),
            ("other operating", "operating_expense"),
            ("other assets", "other_assets"),
            ("operating income", "operating_income"),
            ("interest expense", "intrest_expense"),
            ("total operating expense", "total_costs"),
            ("total costs and expenses", "total_costs"),
            ("preferred dividend", "preferred_dividend"),
            ("income before", "income_before_taxes"),
            ("provision for", "provision_taxes"),
            ("net income from continuing op", "net_income_continuing_ops"),
            ("net income from discontinuing ops", "net_income_discontinuing_ops"),
            ("net income available to common shareholders", "net_income_common"),
            ("net income", "net_income"),
            ("eps basic", "eps_basic"),
            ("eps diluted", "eps_diluted"),
            ("waso basic", "waso_basic"),
            ("waso diluted", "waso_diluted"),
            ("ebitda", "ebitda"),
        ))

        self.special_key_titles = tuple((
            ("key ratios -> profitability", "pro "),
            ("key ratios -> growth", "gro "),
            ("key ratios -> cash flow", "cf "),
            ("key ratios -> financial health", "fh "),
            ("key ratios -> efficiency ratios", "er "),
            ("revenue %", "r% "),
            ("operating income %", "oi% "),
            ("net income %", "ni% "),
            ("eps %", "eps% "),
        ))
        self.special_financials_titles = tuple((
            ("earnings per share", "eps "),
            ("weighted average shares outstanding", "waso "),
        ))

        self.translation_table = dict.fromkeys(map(ord, '",'), None)

    def most_recent_quarter(self):
        day = self.today.day 
        quarter = (self.today.month - 1) // 3
        year = self.today.year
        month = quarter * 3 + 1
        return datetime(year=year, month=month, day=1).date().isoformat()

    def find_column(self, col, mapper, subtitle=''):
        col = col.lower().replace('"', '')
        wst = subtitle + col
        alt = ''
        
        for k, v in mapper:
            if wst.startswith(k):
                return v
            elif col.startswith(k):
                alt = v

        return alt

    def convert_numerical(self, n):
        try:
            return int(n)
        except ValueError:
            try:
                return float(n)
            except ValueError:
                return n

    def get_title_multiplier(self, title):
        multipliers = ["Ths", "Mil", "Bil"]
        factors     = [10**3, 10**6, 10**9]
        for i, multi in enumerate(multipliers):
            if title.endswith(multi):
                return factors[i]

        return 1
        
            
    def parse_csv(self, csv_r, num_cols, special_titles, column_map, start_dic={}):
        subhead = ''
        next(csv_r) # skip headers

        return_dics = []

        for cols in csv_r:
            row_cols = len(cols)
            if row_cols == 0:
                continue
            elif row_cols == 1:
                subhead = self.find_column(cols[0], special_titles)
            else:
                db_col = self.find_column(cols[0], column_map, subtitle=subhead)
                if db_col:
                    multi = self.get_title_multiplier(cols[0])

                    if len(return_dics) == 0:
                        return_dics = [start_dic.copy() for _ in range(num_cols)]

                    for i in range(num_cols):
                        cell = cols[i+1].translate(self.translation_table)
                        val = self.convert_numerical(cell) * multi if cell else None
                        if db_col in self.year_month_cols:
                            val = self.ttm_string if val == 'TTM' else datetime.strptime(val, '%Y-%m')
                        return_dics[i][db_col] = val
        return return_dics

    def get_key_stats(self, ticker, db_exchange="TSX"):
        """
        This function get key statistics from
        Morning Star.
        """
        url = ("http://financials.morningstar.com/ajax/exportKR2CSV.html?t={}&"
               "culture=en-CA&region=CAN&order=asc&r={}"
           ).format(ticker, randint(1, 500000))
        req =  urllib.request.Request(url, headers=self.headers)
        resp = urllib.request.urlopen(req)
        csv_r = csv.reader(codecs.iterdecode(resp, 'utf-8'))

        on_morningstar = csv_r and resp.headers['content-length'] != '0'
        if on_morningstar:
            LOGGER.info("Getting key statistics for {}... ".format(ticker))
        else:
            LOGGER.info("Skipping {}".format(ticker))
            return 1

        return_dics = self.parse_csv(csv_r, 10, self.special_key_titles, self.column_key_map, start_dic = {
            "ticker": ticker,
            "exchange": db_exchange,
            "update_date": self.today
        })

        for d in return_dics:
            stmt = insert(MorningStarKeyStatistics).values(d).on_conflict_do_update(
                constraint = 'ms_key_statistics_pkey',
                set_ = d
            )
            self.session.execute(stmt)

        self.session.commit()

        LOGGER.info("Done")
        return 0
    
    def get_financial(self, ticker, period_name, exchange="XTSE"):
        """
        This function get yearly and quartly information from
        Morning Star.
        
        period_name: "quarter" or "annual"
        exchanges: XTSE (TSX),
        """

        # this converts the morning star exchange name to our database name
        if exchange in self.exchange_map:
            db_exchange = self.exchange_map[exchange]
        else:
            raise ValueError("Exchange unsupported {}".format(exchange))
        
        period = 3 if period_name == "quarter" else 12
        
        url = ("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t="
               "{}:{}&region=can&culture=en-US&cur=&reportType=is&period={}&"
               "dataType=A&order=desc&columnYear=5&curYearPart=1st5year&"
               "rounding=1&view=raw&r={}&denominatorView=raw&number=1"
           ).format(exchange, ticker, period, randint(1, 500000))
        req =  urllib.request.Request(url, headers=self.headers)

        
        resp = urllib.request.urlopen(req)
        csv_r = csv.reader(codecs.iterdecode(resp, 'utf-8'))

        on_morningstar = csv_r and resp.headers['content-length'] != '0'

        if on_morningstar:
            LOGGER.info("Getting {} financial data for {}... ".format(period_name, ticker))
        else:
            LOGGER.info("Skipping {}".format(ticker))
            return 1

        num_cols = 6 if period_name == "quarter" else 5 # skip last column if not quarter view (removes TTM)
        return_dics = self.parse_csv(csv_r, num_cols, self.special_financials_titles,
                                    self.column_financials_map,
                                    start_dic={"ticker": ticker, "exchange": db_exchange,
                                               "period": period, "update_date": self.today})


        for d in return_dics:
            stmt = insert(MorningStarFinancials).values(d).on_conflict_do_update(
                constraint = 'fiscal_year_unique',
                set_ = d
            )
            self.session.execute(stmt)

        self.session.commit()

        LOGGER.info("Done")
        return 0

    def fetch_all(self, db_exchange):
        q = self.session.query(Listings).filter(Listings.exchange == db_exchange, or_(Listings.onms == True, Listings.onms is None))

        for listing in q:
            ticker = listing.ticker
            found1 = mss.get_key_stats(ticker)
            found2 = mss.get_financial(ticker, "quarter")
            found3 = mss.get_financial(ticker, "annual")
            on_morningstar = not(found1 and found2 and found3)  # if the statistics or the financial data
            self.session.query(Listings).filter(Listings.exchange == db_exchange, Listings.ticker == ticker).update({Listings.onms:on_morningstar})

    def clean_exit(self):
        self.session.close()

if __name__ == "__main__":
    mss = MorningStarScaper()
    mss.fetch_all("TSX")
    mss.clean_exit()
