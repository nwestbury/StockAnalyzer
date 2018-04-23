#!/usr/bin/env python3

from sa.database import Database

class Features():

    def __init__(self, normalize=True):
        self.db = Database()
        self.ms_financials_cols = self.db.get_col_names("ms_financials")

    def fetch(self, ticker):
        self.db.cursor.execute("SELECT * from ms_financials LIMIT 1")
        res = self.db.cursor.fetchall()


if __name__ == "__main__":
    fc = Features()
    fc.fetch("IMO")
