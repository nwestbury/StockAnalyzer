#!/usr/bin/env python3

from sa.common import get_ticker_names
from basebot import BaseBot
import random


class DartBot(BaseBot):

    """
    Simplest bot, just returns a random ticker name
    """

    def guess(self):
        ticker_names = get_ticker_names(self.sess, 'TSX')
        return random.choice(ticker_names)


if __name__ == "__main__":
    bot = DartBot('Blind Dart Thrower')
    json_guess = bot()
    print(json_guess)
