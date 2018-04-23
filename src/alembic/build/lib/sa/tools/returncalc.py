from datetime import datetime, timedelta

from sa.database import Database
from sa.logger import LOGGER

class ReturnCalcuator():
    def __init__(self):
        self.db = Database()
        
    def next_business_day(self, ticker, date, before=True, fields="date"):
        cmp = "<=" if before else ">="
        return self.db.select(fields, "price_history",
             where="ticker = %s AND date {} %s ORDER BY date DESC LIMIT 1".format(cmp),
             vals = [ticker, date], fetch='one')
        
    def calculate_return(self, ticker, start_date, end_date):
        min_date = self.db.select("MIN(date)", "price_history", where="ticker = %s", vals=[ticker], fetch='one', unroll=True)
        max_date = self.db.select("MAX(date)", "price_history", where="ticker = %s", vals=[ticker], fetch='one', unroll=True)

        if min_date is None or max_date is None or start_date < min_date:
            return None
        
        start_date, start_close = self.next_business_day(ticker, start_date, fields='date, "adj close"',
                                            before=(start_date >= min_date))
        end_date, end_close = self.next_business_day(ticker, end_date, fields='date, "adj close"',
                                            before=(end_date > max_date))

        return end_close / start_close - 1
        

if __name__ == "__main__":
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=14*365)
    
    rc = ReturnCalcuator()
    rt = rc.calculate_return("IMO", start_date, end_date)

    print("Got return", rt)
