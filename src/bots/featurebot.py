#!/usr/bin/env python3

from basebot import BaseBot
from feature_helper import FeatureHelper
from sa.logger import LOGGER

import numpy as np

from sklearn.svm import SVR
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.feature_selection import SelectKBest, f_regression
import matplotlib.pyplot as plt

class FeatureBot(BaseBot):

    """
    Linear Regression Bot: finds linear patterns in all
    the market data.
    """
    def __init__(self):
        BaseBot.__init__(self)
        self.kbest = SelectKBest(f_regression, k=5)
        self.fh = FeatureHelper(self.sess)

    def graph(self, scores, names):
        fig, ax = plt.subplots()


        full_name = {
            'ro_assets': 'Return on\nAssets',
            'revenue_perc_yoy': 'Revenue %\nYOY',
            'ro_equity': 'Return on\nEquity',
            'eps_5y': '5y EPS',
            'inventory_turnover': 'Inventory\nTurnover',
            'tax_rate': 'Tax Rate',
            'margin_net_income': 'Margin Net\n Income',
            'operating_income_yoy': 'Operating\n Income YOY',
            'other_cur_assets': 'Other Current\n Assets',
            'net_ppe': 'Net PPE',
        }
        top_names = names[:3] + names[-3:]
        top_names = [full_name[name] for name in top_names]


        r1 = ax.bar(np.arange(0, 3), scores[:3], 0.5,
                alpha=.9, color='g', label='Top 3 Features')
        r2 = ax.bar(np.arange(3, 6), scores[-3:], 0.5,
                alpha=.9, color='r', label='Worst 3 Features')

        ax.set_xlabel('Feature Name', fontsize=12)
        ax.set_ylabel('Feature Score (Regression)', fontsize=12)
        ax.set_title('Best and Worst Features by Regression', fontsize=18)
        ax.set_xticks(np.arange(0, 6))
        ax.set_xticklabels(top_names)
        ax.legend()

        fig.tight_layout()
        plt.show()

    def train(self):
        LOGGER.info("Starting to train...")

        train_data, train_targets, test_data, test_targets = self.fh.fetch_feature_data()
        tickers = self.fh.fetch_feature_tickers()

        print("Shapes", train_data.shape, train_targets.shape, test_data.shape, test_targets.shape)

        features_names = self.fh.fetch_feature_names()
        self.kbest.fit(train_data, train_targets)

        feature_scores = self.kbest.scores_

        combined = [tuple((s,f)) for s, f in zip(feature_scores, features_names) if not np.isnan(s)]
        best_features = sorted(combined, key=lambda x: -x[0])
        scores, names = zip(*best_features)

        self.graph(scores, names)
        print('Best Features', scores[:5])
        print('Worst Features', best_features[-5:])

    @property
    def name(self):
        return "SVR Bot"

    def guess(self):
        return self.feature_fetcher.fetch("IMO")


if __name__ == "__main__":
    bot =  FeatureBot()
    bot.train()
