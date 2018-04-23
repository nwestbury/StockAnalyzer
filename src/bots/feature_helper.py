from sa.common import get_ms_ticker_names, find_small_cap_tickers
from sa.tools.features import Features
from sa.tools.returncalc import ReturnCalculator
from sa.logger import LOGGER

import os
import numpy as np

from sklearn.preprocessing import Imputer

class FeatureHelper():
    def __init__(self, session):
        self.dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cache')
        self.file_path = os.path.join(self.dir_path, 'features.npz')
        self.sess = session
        self.ff = Features()

    def fetch_feature_data(self):
        if not os.path.isfile(self.file_path):
            self.generate_and_save_feature_data()

        npz = np.load(self.file_path)

        return tuple(npz[a] for a in ('train_data', 'train_targets', 'test_data', 'test_targets'))

    def fetch_binary_feature_data(self, p=None):
        train_data, train_targets, test_data, test_targets = self.fetch_feature_data()

        median = np.median(train_targets)
        print('meadian is ', median)
        train_targets = np.array([t >= median for t in train_targets])
        test_targets = np.array([t >= median for t in test_targets])

        return train_data, train_targets, test_data, test_targets

    def fetch_feature_tickers(self):
        npz = np.load(self.file_path)

        return npz['train_ticker_names'], npz['test_ticker_names']

    def screen_and_save_feature_data(self):
        train_ticker_names, test_ticker_names = self.fetch_feature_tickers()
        train_data, train_targets, test_data, test_targets = self.fetch_feature_data()

        tickers = set(find_small_cap_tickers(self.sess)) # finds ticker < 10m value
        train_rm_indexes = []
        for i, (ticker, target) in enumerate(zip(train_ticker_names, train_targets)):
            if target > 10 or ticker in tickers:
                train_rm_indexes.append(i)
        test_rm_indexes = []
        for i, (ticker, target) in enumerate(zip(test_ticker_names, test_targets)):
            if target > 10 or ticker in tickers:
                test_rm_indexes.append(i)

        train_ticker_names = np.delete(train_ticker_names, train_rm_indexes, axis=0)
        train_data = np.delete(train_data, train_rm_indexes, axis=0)
        train_targets = np.delete(train_targets, train_rm_indexes, axis=0)
        test_ticker_names = np.delete(test_ticker_names, test_rm_indexes, axis=0)
        test_data = np.delete(test_data, test_rm_indexes, axis=0)
        test_targets = np.delete(test_targets, test_rm_indexes, axis=0)

        LOGGER.info("Saving file at: {}".format(self.file_path))

        np.savez(self.file_path,
                 train_data = train_data, train_targets = train_targets,
                 train_ticker_names = train_ticker_names, test_data = test_data,
                 test_targets = test_targets, test_ticker_names = test_ticker_names)

    def fetch_feature_names(self):
        return self.ff.ms_key_stats_cols

    def generate_and_save_feature_data(self, independent=True):
        rc = ReturnCalculator()

        ticker_names = sorted(get_ms_ticker_names(self.sess, "TSX"))
        num_tickers = len(ticker_names)

        train_data, train_targets = [], []
        train_ticker_names = []
        test_data, test_targets = [], []
        test_ticker_names = []

        imp = Imputer(missing_values='NaN', strategy='mean', axis=0)

        for i, t in enumerate(ticker_names, 1):
            LOGGER.info("[{:d}/{:d}] Working on {}...".format(i, num_tickers, t))

            dates = self.ff.ms_key_stats_date(self.sess, t)

            if len(dates) < 1:
                continue

            date_gap = dates[1] - dates[0] if len(dates) > 2 else timedelta(days=365)
            last_date = dates[-1]

            rows = self.ff.ms_key_stats_data(self.sess, t)

            if not independent:
                # Window sliding for time series
                empty_row = tuple((None,)) * len(rows[0])
                new_rows = []
                for i in range(len(rows)):
                    first_part = rows[i-1] if i > 0 else empty_row
                    second_part = rows[i]
                    new_rows.append(first_part + second_part)
                rows = new_rows

            # Add the start date to the list of dates
            return_dates = [dates[0] - date_gap] + dates

            returns = rc.calculate_return_between_dates(t, return_dates)
            for row, date, ret in zip(rows, dates, returns):
                if ret is None: # if return date are out of range
                    continue

                if date == last_date:
                    test_data.append(row)
                    test_targets.append(ret)
                    test_ticker_names.append(t)
                else:
                    train_data.append(row)
                    train_targets.append(ret)
                    train_ticker_names.append(t)

        # Convert the python lists to numpy arrays and fill missing values
        train_data = np.array(train_data, dtype=np.float)
        imp = imp.fit(train_data)

        train_ticker_names = np.array(train_ticker_names, dtype=np.str)
        train_data = imp.transform(train_data)
        train_targets = np.array(train_targets, dtype=np.float)
        test_ticker_names = np.array(test_ticker_names, dtype=np.str)
        test_data = imp.transform(np.array(test_data, dtype=np.float))
        test_targets = np.array(test_targets, dtype=np.float)

        if not os.path.exists(self.dir_path):
            os.makedirs(self.dir_path)

        LOGGER.info("Saving file at: {}".format(self.file_path))

        np.savez(self.file_path,
                 train_data = train_data, train_targets = train_targets,
                 train_ticker_names = train_ticker_names, test_data = test_data,
                 test_targets = test_targets, test_ticker_names = test_ticker_names)


if __name__ == "__main__":
    from sa.database import Session

    sess = Session()
    fc = FeatureHelper(sess)
    fc.generate_and_save_feature_data(independent=False)
    fc.screen_and_save_feature_data()

