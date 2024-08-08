from elson.data_clean.rules import Rule
from elson.utils.cleansing_utils import Origin_Rule
from pyspark.sql import DataFrame

class Float_Rule(Rule):

    def exec(self, df: DataFrame,origin_rule: Origin_Rule, col: str):
        pass