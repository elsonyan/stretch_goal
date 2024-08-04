# from pyspark.sql import DataFrame, functions as F
from elson.data_clean.rules import Rule, RateRule
from elson.data_clean.utils import OriginRule, load_yaml_rules, Queue
from dataclasses import dataclass


@dataclass
class DataFrame:
    data: int


# 把解析出来的加载成Rule()
def load_rule(rule_detail: OriginRule) -> Rule:
    _rule = RateRule()
    for prop in dir(rule_detail):
        if not prop.startswith('__'):  # except __ func
            attr = getattr(rule_detail, prop)
            setattr(_rule, prop, attr)
    return _rule


#
def load_rules(origin_rules, *match_rules: str) -> Queue:
    rule_queue = Queue()

    # 按 先后顺序，将rule排列在队列中。实现优先级
    def extract_rule(_rule):
        # get rule by
        attr = getattr(origin_rules, _rule)
        # if get a list , flatten list to get the truly clean operation
        if isinstance(attr, list):
            for r in attr:
                extract_rule(r)
        else:
            # if get a class , must be clean operation
            if attr.__class__.__name__ == OriginRule.__name__:
                rule_queue.append(load_rule(attr))
            else:
                extract_rule(attr)

    for rule in match_rules:
        extract_rule(rule)
    return rule_queue


@dataclass
class exec_plan:
    rules: Queue = None
    columns: tuple = None

    def exec(self, dataframe: DataFrame) -> DataFrame:
        while True:
            shift: Rule = self.rules.shift
            if not shift:
                break
            dataframe = DataFrame(dataframe.data + 1)
            print(dataframe, shift.__dict__, self.columns)
        return dataframe


class Cleansing:
    def __init__(self,
                 df: DataFrame,
                 yaml_path: str):
        self.df: DataFrame = df
        self.origin_rules: OriginRule = load_yaml_rules(yaml_path)
        self.exec_plan_list: Queue = Queue()
        self.rule_step: Queue = Queue()
        self.column_step: Queue = Queue()

    def rule(self, *rules: str):
        self.rule_step.append(rules)
        return self

    def column(self, *columns: str):
        self.column_step.append(columns)
        return self

    def zip_rule_cols(self):
        if self.rule_step.size != self.column_step.size:
            raise Exception("Rule and Column plans are not matching")
        size = self.column_step.size
        for _ in range(size):
            rule = self.rule_step.shift
            column = self.column_step.shift
            rule_sorted = load_rules(self.origin_rules, *rule)
            self.exec_plan_list.append(exec_plan(rule_sorted, column))

    def exec(self):
        self.zip_rule_cols()
        while True:
            plan: exec_plan = self.exec_plan_list.shift
            if not plan:
                break
            self.df = plan.exec(self.df)


if __name__ == '__main__':
    df: DataFrame = DataFrame(0)
    # rate_df = spark.createDataFrame(data=[{'name': 'Alice', 'age': 20}])
    cleansing = Cleansing(df, r"C:\Users\93134\Desktop\stretch_goal\src\elson\data_clean\rules.yaml")
    cleansing.rule("Rule3").rule("BaseRule2") \
        .column("col1").column("col2") \
        .exec()
