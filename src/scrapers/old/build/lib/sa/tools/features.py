#!/usr/bin/env python3

from sa.models import MorningStarFinancials, MorningStarKeyStatistics, YahooKeyStatistics

class Features():
    def __init__(self, sess):
        ms_financial_keys = set(MorningStarFinancials.__table__.columns.keys())
        ms_key_stats_keys = set(MorningStarKeyStatistics.__table__.columns.keys())
        yahoo_key_stats_keys = set(YahooKeyStatistics.__table__.columns.keys())

        self.ms_financials_cols = sorted(ms_financial_keys - {'ticker', 'exchange', 'update_date', 'fiscal_year'})
        self.ms_key_stats_cols = sorted(ms_key_stats_keys - {'ticker', 'exchange', 'profitability_date', 'margin_date', 'update_date'})
        self.yahoo_key_stats_cols = sorted(yahoo_key_stats_keys - {'ticker', 'exchange'})

    def ms_finance_fetch(self, sess, ticker, lower_date, upper_date, period='3'):
        q = sess.query(*[getattr(MorningStarFinancials, c) for c in self.ms_financials_cols]).filter(
            MorningStarFinancials.period == '3', MorningStarFinancials.ticker == ticker,
            MorningStarFinancials.fiscal_year.between(lower_date, upper_date),
        )
        rows = sess.execute(q).fetchall()

        return rows

    def ms_key_stats_date(self, sess, ticker):
        """Returns a ascending list of dates for a ticker in the
        MoringStarKeyStatistics table
        """
        q = sess.query(MorningStarKeyStatistics.profitability_date).filter(
            MorningStarKeyStatistics.ticker == ticker
        ).order_by(MorningStarKeyStatistics.profitability_date.asc())

        rows = sess.execute(q).fetchall()

        return [d for d, in rows]

    def ms_key_stats_data(self, sess, ticker):
        q = sess.query(*[getattr(MorningStarKeyStatistics, c) for c in self.ms_key_stats_cols]).filter(
            MorningStarKeyStatistics.ticker == ticker,
        ).order_by(MorningStarKeyStatistics.profitability_date.asc())
        rows = sess.execute(q).fetchall()

        return [tuple(d) for d in rows]

if __name__ == "__main__":
    fc = Features()
    fc.fetch("IMO")
