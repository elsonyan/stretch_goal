import json
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, functions as F
from enum import Enum


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


class Rule(ABC):
    def __init__(self):
        self.name: str = self.__class__.__name__

    @abstractmethod
    def exec(self, df: DataFrame, col: str):
        raise NotImplemented


class Rate_rule(Rule):

    def exec(self, df: DataFrame, col: str) -> DataFrame:
        return df.withColumn(col, F.concat(F.col(col).cast("string"), F.lit(self.name)))
