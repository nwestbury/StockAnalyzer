#!/usr/bin/env python3

from basebot import BaseBot
from feature_helper import FeatureHelper
from sa.logger import LOGGER

from datetime import datetime, timedelta, date
from math import ceil
import random
import itertools

from sklearn.linear_model import LinearRegression, SGDClassifier
from sklearn.metrics import accuracy_score, roc_auc_score, mean_absolute_error, mean_squared_error

class LinearBot(BaseBot):

    """
    Linear Regression Bot: finds linear patterns in all
    the market data.
    """
    def __init__(self):
        BaseBot.__init__(self)
        self.lr = LinearRegression()
        self.lc = SGDClassifier()
        self.fh = FeatureHelper(self.sess)

    def train(self):
        LOGGER.info("Starting to train...")

        train_data, train_targets, test_data, test_targets = self.fh.fetch_feature_data()
        train_tickers, test_tickers = self.fh.fetch_feature_tickers()

        print("Shapes", train_data.shape, train_targets.shape, test_data.shape, test_targets.shape)

        self.lr.fit(train_data, train_targets)

        predictions = self.lr.predict(test_data)

        mean_err = mean_absolute_error(test_targets, predictions)
        mean_s_err = mean_squared_error(test_targets, predictions)


        print('TESt', sorted(zip(test_targets, predictions, test_tickers), key=lambda x: -x[1])[:10])

        print("Got Mean error", mean_err, "Squared Err", mean_s_err)
        print("Average Expected Return", sum(predictions) / len(predictions))
        print("Average True Return", sum(train_targets) / len(train_targets))

    def binary_train(self):
        LOGGER.info("Starting to train...")

        train_data, train_targets, test_data, test_targets = self.fh.fetch_binary_feature_data()
        train_tickers, test_tickers = self.fh.fetch_feature_tickers()

        print("Shapes", train_data.shape, train_targets.shape, test_data.shape, test_targets.shape)

        self.lc.fit(train_data, train_targets)

        predictions = self.lc.predict(test_data)

        acc_score = accuracy_score(test_targets, predictions)
        roc_score = roc_auc_score(test_targets, predictions)
        print("Accuracy Score", acc_score, 'ROC Score', roc_score)
        print("Average True Return", sum(train_targets) / len(train_targets), sum(test_targets) / len(test_targets))

    @property
    def name(self):
        return "Line Rider"

    def guess(self):
        return self.feature_fetcher.fetch("IMO")


if __name__ == "__main__":
    bot = LinearBot()

    bot.binary_train()

    # json_guess = bot()
    # print(json_guess)
