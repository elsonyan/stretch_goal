from elson.data_clean.rules.BaseRule import Rate_Rule, Plan_type, Rule
from elson.data_clean.rules.BigIntRule import BigInt_Rule
from elson.data_clean.rules.BoolRule import Bool_Rule
from elson.data_clean.rules.CharRule import Char_Rule
from elson.data_clean.rules.DateRule import Date_Rule
from elson.data_clean.rules.DoubleRule import Double_Rule
from elson.data_clean.rules.FloatRule import Float_Rule
from elson.data_clean.rules.IntRule import Int_Rule
from elson.data_clean.rules.StringRule import String_Rule
from elson.data_clean.rules.TimestampRule import Timestamp_Rule


def match_rule(plan_type: Plan_type) -> Rule.__class__:
    # base_module = "elson.data_clean.rules"
    # module = import_module(base_module)
    s = str(plan_type)
    result = Rate_Rule
    if s == "rate":
        result = Rate_Rule
    elif s == "string":
        result = String_Rule
    elif s == "bigint":
        result = BigInt_Rule
    elif s == "int":
        result = Int_Rule
    elif s == "boolean":
        result = Bool_Rule
    elif s == "date":
        result = Date_Rule
    elif s == "timestamp":
        result = Timestamp_Rule
    elif s == "char":
        result = Char_Rule
    elif s == "double":
        result = Double_Rule
    elif s == "float":
        result = Float_Rule

    return result
