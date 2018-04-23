#!/usr/bin/env python3

from basebot import BaseBot
from feature_helper import FeatureHelper
from sa.logger import LOGGER

from datetime import datetime, timedelta, date
from math import ceil
import random
import itertools

from sklearn.metrics import mean_absolute_error, mean_squared_error

class MeanBot(BaseBot):

    """
    Mean Bot: Predict the average
    """
    def __init__(self):
        BaseBot.__init__(self)
        self.fh = FeatureHelper(self.sess)

    def train(self):
        LOGGER.info("Starting to train...")

        train_data, train_targets, test_data, test_targets = self.fh.fetch_feature_data()
        train_tickers, test_tickers = self.fh.fetch_feature_tickers()

        print("Shapes", train_data.shape, train_targets.shape, test_data.shape, test_targets.shape)

        simple_preds = [sum(train_targets) / len(train_targets)] * len(test_targets)
        mean_err = mean_absolute_error(test_targets, simple_preds)
        mean_s_err = mean_squared_error(test_targets, simple_preds)

        print("Got Mean error", mean_err, "Squared Err", mean_s_err)
        print("Average Expected Return", sum(train_targets) / len(train_targets))
        print("Average True Return", sum(train_targets) / len(train_targets))

    @property
    def name(self):
        return "Line Rider"

    def guess(self):
        return self.feature_fetcher.fetch("IMO")


if __name__ == "__main__":
    bot = MeanBot()

    bot.train()
    
    # json_guess = bot()
    # print(json_guess)
