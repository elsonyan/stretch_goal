import json
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, functions as F
from enum import Enum
import importlib


# this module include all rules .
# Define multiple rules. Each field type has its own operation logic, specific functions or properties.

class Plan_type(Enum):
    RATE = "rate"
    STRING = "string"
    BIGINE = "bigint"
    INT = "int"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    CHAR = "char"
    DOUBLE = "double"

    def __str__(self) -> str:
        return str(self.value)


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
    def exec(self, df: DataFrame, col: F.col):
        raise NotImplemented


class RateRule(Rule):

    def exec(self, df: DataFrame, col: F.col):
        return df.withColumn(col, col + self.name)


class StringRule(Rule):
    def exec(self, df: DataFrame, col: F.col):
        pass


class BigIntRule(Rule):
    def exec(self, df: DataFrame, col: F.col):
        pass


class IntRule(Rule):
    def exec(self, df: DataFrame, col: F.col):
        pass


class BoolRule(Rule):
    def exec(self, df: DataFrame, col: F.col):
        pass


class DateRule(Rule):
    def exec(self, df: DataFrame, col: F.col):
        pass


class TimestampRule(Rule):
    def exec(self, df: DataFrame, col: F.col):
        pass


class CharRule(Rule):
    def exec(self, df: DataFrame, col: F.col):
        pass


class DoubleRule(Rule):
    def exec(self, df: DataFrame, col: F.col):
        pass


class FloatRule(Rule):
    def exec(self, df: DataFrame, col: F.col):
        pass


def match_plan(plan_type: Plan_type) -> Rule.__class__:
    # base_module = "elson.data_clean.rules"
    # module = importlib.import_module(base_module)
    s = str(plan_type)
    result = RateRule
    if s == "rate":
        result = RateRule
    elif s == "string":
        result = StringRule
    elif s == "bigint":
        result = BigIntRule
    elif s == "int":
        result = IntRule
    elif s == "boolean":
        result = BoolRule
    elif s == "date":
        result = DateRule
    elif s == "timestamp":
        result = TimestampRule
    elif s == "char":
        result = CharRule
    elif s == "double":
        result = DoubleRule

    return result


if __name__ == '__main__':
    plan_type = Plan_type("string")
    print(plan_type)
    plan = match_plan(plan_type)
    print(plan())
