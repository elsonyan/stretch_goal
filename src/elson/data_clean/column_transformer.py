from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from elson.data_clean.rules import Rule, StringRule, RateRule
from elson.data_clean.utils import OriginRule, init_yaml_rules, RuleQueue


def AnalysisRule(rule_detail: OriginRule) -> Rule:
    rule = RateRule()
    for prop in dir(rule_detail):
        if not prop.startswith('__'):  # except __ func
            attr = getattr(rule_detail, prop)
            setattr(rule, prop, attr)
    return rule


def SortRules(origin_rule: OriginRule, *match_rules: str) -> RuleQueue:
    rule_queue = RuleQueue()

    def extract_rule(rule):
        attr = getattr(origin_rule, rule)
        # if get a list , flatten list to get the truly clean operation
        if isinstance(attr, list):
            for r in attr:
                extract_rule(r)
        else:
            # if get a class , must be clean operation
            if OriginRule.__class__.__name__ == OriginRule.__class__.__name__:
                rule_queue.append(AnalysisRule(attr))
            else:
                extract_rule(attr)

    for rule in match_rules:
        extract_rule(rule)

    return rule_queue


def CleanDF(df: DataFrame, rule_list: [Rule]) -> DataFrame:
    pass


if __name__ == '__main__':
    origin_rule = init_yaml_rules(r"rules.yaml")
    from pprint import pprint

    # print(str(origin_rule))
    rules = SortRules(origin_rule, "Rule2", "Rule1")
    rules.list
