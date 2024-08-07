from elson.data_clean.rules.BaseRule import Rate_rule, Plan_type, Rule
from elson.data_clean.rules.BigIntRule import BigInt_rule
from elson.data_clean.rules.BigIntRule import BigInt_rule
from elson.data_clean.rules.BoolRule import Bool_rule
from elson.data_clean.rules.CharRule import Char_rule
from elson.data_clean.rules.DateRule import Date_rule
from elson.data_clean.rules.DoubleRule import Double_rule
from elson.data_clean.rules.FloatRule import Float_rule
from elson.data_clean.rules.IntRule import Int_rule
from elson.data_clean.rules.StringRule import String_rule
from elson.data_clean.rules.TimestampRule import Timestamp_rule


def match_rule(plan_type: Plan_type) -> Rule.__class__:
    # base_module = "elson.data_clean.rules"
    # module = import_module(base_module)
    s = str(plan_type)
    result = Rate_rule
    if s == "rate":
        result = Rate_rule
    elif s == "string":
        result = String_rule
    elif s == "bigint":
        result = BigInt_rule
    elif s == "int":
        result = Int_rule
    elif s == "boolean":
        result = Bool_rule
    elif s == "date":
        result = Date_rule
    elif s == "timestamp":
        result = Timestamp_rule
    elif s == "char":
        result = Char_rule
    elif s == "double":
        result = Double_rule
    elif s == "float":
        result = Float_rule

    return result
