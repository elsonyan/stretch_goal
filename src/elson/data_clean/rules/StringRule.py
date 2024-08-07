from elson.data_clean.rules import Rule
from pyspark.sql import DataFrame


class String_rule(Rule):

    def exec(self, df: DataFrame, col: str):
        pass