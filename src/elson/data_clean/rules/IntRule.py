from elson.data_clean.rules._rule import Rule
from pyspark.sql import DataFrame


class Int_rule(Rule):

    def exec(self, df: DataFrame, col: str):
        pass