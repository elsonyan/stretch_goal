from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from elson.data_clean.rules import Rule
from elson.data_clean.utils import OriginRule, init_yaml_rules, RuleQueue


def AnalysisRule(rule_detail:Rule) -> Rule:
    pass
def SortRules(origin_rule: OriginRule, *match_rules: str) -> RuleQueue:
    rule_queue = RuleQueue()
    def extract_rule(rule):
        attr = getattr(origin_rule, rule)
        print(attr)
        if isinstance(attr,list):
            for r in attr:
                extract_rule(r)
        else:
            print(attr)
            if issubclass(attr,Rule):
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
    SortRules(origin_rule, "Rule2", "Rule1").list()
