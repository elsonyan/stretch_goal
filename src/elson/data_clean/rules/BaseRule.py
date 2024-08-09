from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import column as C
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

    def convert_null_2_str(self, col) -> C.Column:
        return F.when(F.col(col).isNull(), "").otherwise(F.col(col))

    def convert_str_2_null(self, col) -> C.Column:
        return F.when("" == F.trim(F.col(col)), None).otherwise(F.col(col))

    def upper_str(self, col) -> C.Column:
        return F.upper(col)

    def exec(self, df: DataFrame, origin_rule: Origin_Rule, col: str) -> DataFrame:
        if not getattr(origin_rule, "operation"):
            raise Exception(f"no operation in {origin_rule}")
        global expr
        if getattr(origin_rule, "operation") == "str_2_null":
            expr = self.convert_str_2_null(col)
        elif getattr(origin_rule, "operation") == "null_2_str":
            expr = self.convert_null_2_str(col)
        elif getattr(origin_rule, "operation") == "upper":
            expr = self.upper_str(col)
        return df.withColumn(col, expr)
