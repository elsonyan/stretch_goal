import json
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, functions as F
from enum import Enum
from importlib import import_module

from elson.data_clean.rules import BigInt_rule, Bool_rule, Char_rule, Date_rule, Double_rule, Float_rule, Int_rule, String_rule, Timestamp_rule


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
        self.name: str = self.__class__.__name__

    @abstractmethod
    def exec(self, df: DataFrame, col: str):
        raise NotImplemented


class Rate_rule(Rule):

    def exec(self, df: DataFrame, col: str) -> DataFrame:
        return df.withColumn(col, F.concat(F.cast(F.col(col), "string"), F.lit(self.name)))


def match_plan(plan_type: Plan_type) -> Rule.__class__:
    base_module = "elson.data_clean.rules"
    module = import_module(base_module)
    s = str(plan_type)
    result = module.Rate_rule
    if s == "rate":
        result = module.Rate_rule
    elif s == "string":
        result = module.String_rule
    elif s == "bigint":
        result = module.BigInt_rule
    elif s == "int":
        result = module.Int_rule
    elif s == "boolean":
        result = module.Bool_rule
    elif s == "date":
        result = module.Date_rule
    elif s == "timestamp":
        result = module.Timestamp_rule
    elif s == "char":
        result = module.Char_rule
    elif s == "double":
        result = module.Double_rule
    elif s == "float":
        result = module.Float_rule

    return result
