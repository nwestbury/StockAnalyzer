#!/usr/bin/env python3

import json
from abc import ABCMeta, abstractmethod

class BaseBot(metaclass=ABCMeta):
    """
    Parent class of all python-based bots, it is presumed that
    the function "guess" will be defined in all children classes.
    """

    @property
    @abstractmethod
    def name(self):
        raise NotImplemented
    
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
