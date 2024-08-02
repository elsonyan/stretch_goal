from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from elson.data_clean.rules import Rule, RateRule
from elson.data_clean.utils import OriginRule, init_yaml_rules, RuleQueue
from typing import Optional


def LoadRule(rule_detail: OriginRule) -> Rule:
    rule = RateRule()
    for prop in dir(rule_detail):
        if not prop.startswith('__'):  # except __ func
            attr = getattr(rule_detail, prop)
            setattr(rule, prop, attr)
    return rule


def SortRules(origin_rule: OriginRule, *match_rules: str) -> RuleQueue:
    rule_queue = RuleQueue()

    def extract_rule(rule):
        # get rule by
        attr = getattr(origin_rule, rule)
        # if get a list , flatten list to get the truly clean operation
        if isinstance(attr, list):
            for r in attr:
                extract_rule(r)
        else:
            # if get a class , must be clean operation
            if attr.__class__.__name__ == OriginRule.__name__:
                rule_queue.append(LoadRule(attr))
            else:
                extract_rule(attr)

    for rule in match_rules:
        extract_rule(rule)

    return rule_queue


def CleanDF(df: DataFrame,
            rule_queue: RuleQueue,
            column: Optional = "*"
            ) -> DataFrame:
    if isinstance(column, list):
        print("list")
    if isinstance(column, str):
        print("str")


if __name__ == '__main__':
    # from pyspark.sql import SparkSession
    # spark = SparkSession.builder.appName("AppName").getOrCreate()
    # rate_df = spark.createDataFrame(data=[{'name': 'Alice', 'age': 20}])
    origin_rule = init_yaml_rules(r"rules.yaml")
    rules = SortRules(origin_rule, "Rule3")
    rate_df = None
    CleanDF(df=rate_df, rule_queue=rules, column=[1,2])
