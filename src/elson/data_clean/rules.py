import json
from abc import ABC, abstractmethod


# this module include all rules .
# Define multiple rules. Each field type has its own operation logic, specific functions or properties.

class OriginRule(object):
    def __init__(self, *args):
        for arg in args:
            for k, v in arg.items():
                if isinstance(v, dict):
                    self.__dict__[k] = OriginRule(v)
                else:
                    self.__dict__[k] = v

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)


class Rule(ABC):
    def __init__(self):
        self.name = self.__class__.__name__

    @abstractmethod
    def exec(self):
        raise NotImplemented


class RateRule(Rule):

    def exec(self):
        pass


class StringRule(Rule):
    pass


class BigIntRule(Rule):
    def exec(self):
        pass


class IntRule(Rule):
    def exec(self):
        pass


class BoolRule(Rule):
    def exec(self):
        pass


class DateRule(Rule):
    def exec(self):
        pass


class TimestampRule(Rule):
    def exec(self):
        pass


class CharRule(Rule):
    def exec(self):
        pass


class DoubleRule(Rule):
    def exec(self):
        pass


class FloatRule:
    def exec(self):
        pass

