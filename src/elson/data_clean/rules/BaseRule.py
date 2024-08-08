from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, functions as F
from enum import Enum

from elson.utils.cleansing_utils import Origin_Rule


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
    def exec(self, df: DataFrame, origin_rule: Origin_Rule, col: str):
        """
        this function is the entry ,
        :param df: source dataframe
        :param origin_rule: the rules in yaml file . parse as a object
        :param col: Single column needs cleaning
        :return: dataframe : like df.withColumn(col, F.concat(F.col(col).cast("string"), F.lit("hello")))
        """
        raise NotImplemented


class Rate_Rule(Rule):

    def exec(self, df: DataFrame, origin_rule: Origin_Rule, col: str) -> DataFrame:
        return df.withColumn(col, F.concat(F.col(col).cast("string"), F.lit(self.name)))
