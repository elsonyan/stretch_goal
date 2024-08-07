from pyspark.sql import DataFrame
from elson.data_clean.rules import Rule, Plan_type,match_rule
from elson.data_clean.utils import OriginRule, load_yaml_rules, Queue, entire_exist
from dataclasses import dataclass


# load yaml obj as a Rule obj
def load_rule(rule_detail: OriginRule) -> Rule:
    _rule = match_rule(getattr(rule_detail, "data_type"))
    for prop in dir(rule_detail):
        if not prop.startswith('__'):  # except __ func
            attr = getattr(rule_detail, prop)
            setattr(_rule, prop, attr)
    return _rule()


#
def parse_rules(origin_rules: object, *match_rules: str) -> Queue:
    rule_queue = Queue()

    # 按 先后顺序，将rule排列在队列中。实现优先级
    def extract_rule(_rule):
        # get rules by
        attr = getattr(origin_rules, _rule)
        # if get a list , flatten list to get the truly clean operation
        if isinstance(attr, list):
            for r in attr:
                extract_rule(r)
        else:
            # if get a class , must be clean operation
            if attr.__class__.__name__ == OriginRule.__name__:
                rule_queue.append(attr)
            else:
                extract_rule(attr)

    for rule in match_rules:
        extract_rule(rule)
    return rule_queue


# execution plan. Execute a plan at each step
@dataclass
class Execution:
    rule: Rule = None
    columns: tuple = None

    def exec(self, df: DataFrame) -> DataFrame:
        def transform(_df: DataFrame, _rule: Rule, _col: str) -> DataFrame:
            return _rule.exec(_df, _col)

        # make sure all columns exists in Dataframe
        col_list = [c[0] for c in df.dtypes if c[1].lower() == getattr(self.rule, "data_type").lower()]
        if getattr(self.rule, "data_type") == str(Plan_type.RATE):
            col_list = list(self.columns)
        if entire_exist(col_list, list(self.columns)):
            for _col in col_list:
                df = transform(df, self.rule, _col)
            return df
        else:
            raise Exception("col not matched in DataFrame")


class Cleansing:
    def __init__(self,
                 df: DataFrame,
                 yaml_path: str):
        self.df: DataFrame = df
        self.origin_rules: OriginRule = load_yaml_rules(yaml_path)
        self.execution_plan: Queue = Queue()
        self.rule_step: Queue = Queue()
        self.column_step: Queue = Queue()

    def add_rule(self, *rules: str):
        ### todo rules should be a Queue
        self.rule_step.append(rules)
        return self

    def add_column(self, *columns: str):
        self.column_step.append(columns)
        return self

    def _arrange_execution_plan(self):
        # Rules and fields to be applied should match. Make sure each batch of fields has corresponding rules.
        if self.rule_step.size != self.column_step.size:
            raise Exception("Rule and Column plans are not matching")
        size = self.column_step.size
        for _ in range(size):
            rule = self.rule_step.shift
            column = self.column_step.shift
            # plan quene , for each 'add_column' step
            rule_sorted = parse_rules(self.origin_rules, *rule)
            while True:
                tmp_plan: OriginRule = rule_sorted.shift
                if not tmp_plan:
                    break
                # make sure rules has data_type attribute
                if not hasattr(tmp_plan, 'data_type'):
                    raise Exception(f"Missed 'data_type' from {tmp_plan}")
                rule:Rule = load_rule(tmp_plan)
                self.execution_plan.append(Execution(rule, column))
        print("total executions:", self.execution_plan.size)

    def show_execution_plan(self):
        self._arrange_execution_plan()
        self.execution_plan.list()

    def exec(self):
        self._arrange_execution_plan()
        while True:
            execution: Execution = self.execution_plan.shift
            if not execution:
                break
            print("execution plan:", execution)
            self.df = execution.exec(self.df)
        return self.df

# if __name__ == '__main__':
#     # rate_df = spark.createDataFrame(data=[{'name': 'Alice', 'age': 20}])
#     @dataclass
#     class DataFrame:
#         data: int

#     rate_df: DataFrame = DataFrame(0)
#     cleansing = Cleansing(rate_df, r"C:\Users\elson.sc.yan\Desktop\stretch_goal\src\elson\data_clean\rules.yaml")
#     cleansing.add_rule("Rule3").add_rule("BaseRule2") \
#         .add_column("col1").add_column("col2") \
#         .exec()
