#!/usr/bin/env python3

from sa.database import Database

class Features():
    def __init__(self, normalize=True):
        self.db = Database()
        self.ms_financials_cols = self.db.get_col_names("ms_financials")
        self.not_feature_cols = {'ticker', 'exchange', 'update_date', 'fiscal_year', 'period'}
        self.ms_financials_cols = [x for x in self.ms_financials_cols if x not in self.not_feature_cols]
        self.ms_financials_cols_str = ','.join(self.ms_financials_cols)

    def fetch(self, ticker, lower_date, upper_date, period='3', where=""):
        cond = "ticker = %s AND period = %s AND fiscal_year >= %s AND fiscal_year <= %s ORDER BY fiscal_year ASC"
        return self.db.select(self.ms_financials_cols_str, "ms_financials",
                              where=cond, vals=[ticker, period, lower_date, upper_date])

if __name__ == "__main__":
    fc = Features()
    fc.fetch("IMO")
