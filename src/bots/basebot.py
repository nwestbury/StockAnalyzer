#!/usr/bin/env python3

import json
from abc import ABCMeta, abstractmethod
from sa.database import Session

class BaseBot(metaclass=ABCMeta):
    """
    Parent class of all python-based bots, it is presumed that
    the function "guess" will be defined in all children classes.
    """

    def __init__(self):
        self.sess = Session()

    @property
    @abstractmethod
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @abstractmethod
    def guess(self):
        raise NotImplemented

    def __call__(self):
        """
        Returns a json string in the appropriate format.
        """
        json_guess = json.dumps({"name": self.name,
                                 "guess": self.guess()})
        return json_guess
