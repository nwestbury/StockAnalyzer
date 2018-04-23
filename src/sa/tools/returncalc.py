from datetime import datetime, timedelta
from operator import le, ge
from sqlalchemy import func, asc, desc
from sqlalchemy.sql import text

from sa.database import Session
from sa.logger import LOGGER
from sa.models import PriceHistory


class ReturnCalculator:
    def __init__(self):
        self.session = Session()

    def close_business_day(self, ticker, date, before=True):
        cmp = le if before else ge  # "<=", ">="
        sort_dir = desc if before else asc

        result = self.session.query(PriceHistory.adj_close).filter(
            PriceHistory.ticker == ticker,
            cmp(PriceHistory.date, date)
        ).order_by(sort_dir(PriceHistory.date)).first()

        return result if result is None else result[0]

    def min_max_dates(self, ticker):
        min_date, = self.session.query(func.min(PriceHistory.date)).filter(PriceHistory.ticker == ticker).first()
        max_date, = self.session.query(func.max(PriceHistory.date)).filter(PriceHistory.ticker == ticker).first()

        return min_date, max_date

    def __calc_return(self, ticker, start_date, end_date):
        start_close, = self.close_business_day(ticker, start_date)
        end_close, = self.close_business_day(ticker, end_date)

        return end_close / start_close - 1

    def calculate_return(self, ticker, start_date, end_date):
        min_date, max_date = self.min_max_dates(ticker)

        if min_date is None or max_date is None:
            return None

        if start_date < min_date or end_date > max_date:
            return None

        ret = self.__calc_return(ticker, start_date, end_date)
        return ret

    def calculate_multi_returns(self, ticker, start_end_dates):
        min_date, max_date = self.min_max_dates(ticker)

        if min_date is None or max_date is None:
            return [None] * len(start_end_dates)

        rets = []
        for start_date, end_date in start_end_dates:
            if start_date < min_date or end_date > max_date:
                ret = None
            else:
                ret = self.__calc_return(ticker, start_date, end_date)
            rets.append(rets)

        return rets

    def calculate_return_between_dates(self, ticker, dates):
        """Efficiently calculates returns between dates. Assumes ascending dates and
           len(dates) > 1.
        """

        min_date, max_date = self.min_max_dates(ticker)

        if min_date is None or max_date is None:
            return [None] * len(dates)

        prev_close = None
        returns = []

        for i, date in enumerate(dates):
            close = None if date < min_date or date > max_date else self.close_business_day(ticker, date)
            if i > 0:
                ret = None if prev_close is None or close is None else close / prev_close - 1
                returns.append(ret)
            prev_close = close

        return returns

if __name__ == "__main__":
    sample_end_date = datetime.today().date()
    sample_start_date = sample_end_date - timedelta(days=20*365)

    rc = ReturnCalculator()
    rt = rc.calculate_return("TD", sample_start_date, sample_end_date)

    print("Got return", rt)
