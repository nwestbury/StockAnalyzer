#!/usr/bin/env python3

from sa.bots.basebot import BaseBot
from sa.bots.features import Features
from sa.common import get_ticker_names
from sa.database import Database
from sa.tools.returncalc import ReturnCalcuator
from sa.logger import LOGGER

from datetime import datetime, timedelta
from math import ceil
import random, itertools

from sklearn.linear_model import LinearRegression

class LinearBot(BaseBot):

    """
    Linear Regression Bot: finds linear patterns in all
    the market data.
    """
    def __init__(self):
        BaseBot.__init__(self)
        self.db = Database()
        self.ff = Features()
        self.rc = ReturnCalcuator()
        
        self.lr = LinearRegression()

        self.training_perc = 0.8
        self.training_tickers = []
        self.training_target = []
        
        self.goal_tickers = []
        self.goal_target = []
        
    def train(self):
        LOGGER.info("Starting to train...")
        
        ticker_names = get_ticker_names(self.db, "TSX")[:10]
        random.shuffle(ticker_names)

        sdate = datetime.today().date() - timedelta(days=2*365)
        edate = sdate + timedelta(days=365)

        data, targets = [], []
        for t in ticker_names:
            c = self.rc.calculate_return(t, sdate, edate)
            if c is None:
                continue

            d = self.ff.fetch(t, sdate, edate)
            d = list(itertools.chain.from_iterable(d))
            
            targets.append(c)
            data.append(d)

        train_tickers = ceil(len(targets) * self.training_perc)

        self.training_data = data[:train_tickers]
        self.training_target  = targets[:train_tickers]
        
        self.goal_data   = data[train_tickers:]
        self.goal_target = targets[train_tickers:]

        LOGGER.info("Starting to train...")
        print("TEST", self.training_data, self.training_target)
        self.lr.fit(self.training_data, self.training_target)

    @property
    def name(self):
        return "Line Rider"
        
    def guess(self):
        return self.feature_fetcher.fetch("IMO")


if __name__ == "__main__":
    bot = LinearBot()

    bot.train()
    
    # json_guess = bot()
    # print(json_guess)
