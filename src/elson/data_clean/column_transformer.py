from pyspark.sql import DataFrame, functions as F
from elson.data_clean.rules import Rule, RateRule
from elson.data_clean.utils import OriginRule, load_yaml_rules, Queue, entire_exist, load_cols
from dataclasses import dataclass


# load yaml obj as a Rule obj
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


# execution plan. Execute a plan at each step
@dataclass
class exec_plan:
    rules: Queue = None
    columns: tuple = None

    def exec(self, dataframe: DataFrame) -> DataFrame:
        def transform(_df: DataFrame, _rule: Rule, _col: str) -> DataFrame:
            return _rule.exec(_df, _col)

        while True:
            current_rule: Rule = self.rules.shift
            if not current_rule:
                break
            # make sure rule has data_type key
            if not hasattr(current_rule, 'data_type'):
                raise Exception(f"Missed 'data_type' from {current_rule.name}")
            # make sure all columns exists in Dataframe
            col_list = [c[0] for c in dataframe.dtypes if c[1].lower() == current_rule.data_type.lower()]
            if entire_exist(col_list, list(self.columns)):
                for _col in col_list:
                    dataframe = transform(dataframe, current_rule, _col)
            else:
                raise Exception("col not matched in DataFrame")
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

    def add_rule(self, *rules: str):
        self.rule_step.append(rules)
        return self

    def add_column(self, *columns: str):
        self.column_step.append(columns)
        return self

    def zip_rule_cols(self):
        # Rules and fields to be applied should match. Make sure each batch of fields has corresponding rules.
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
        return self

if __name__ == '__main__':
    # rate_df = spark.createDataFrame(data=[{'name': 'Alice', 'age': 20}])
    @dataclass
    class DataFrame:
        data: int


    rate_df: DataFrame = DataFrame(0)
    cleansing = Cleansing(rate_df, r"C:\Users\elson.sc.yan\Desktop\stretch_goal\src\elson\data_clean\rules.yaml")
    cleansing.add_rule("Rule3").add_rule("BaseRule2") \
        .add_column("col1").add_column("col2") \
        .exec()
