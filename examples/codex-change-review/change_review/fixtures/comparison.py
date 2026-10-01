"""Fixed before/after measurements; never supplied by the agent (MIT)."""
import importlib.util
import json
from pathlib import Path

CASES = ((9999, 500), (10000, 0), (10001, 0))


def error_result(error):
    message = (type(error).__name__ + ": " + str(error)).encode("utf-8", errors="replace")
    return {"value": None, "error": message[:512].decode("utf-8", errors="ignore")}


def measure(filename):
    try:
        spec = importlib.util.spec_from_file_location("shipping_" + filename, Path(__file__).with_name(filename + ".py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        function = module.shipping_cost
    except BaseException as error:
        return [error_result(error) for _ in CASES]
    values = []
    for total, _expected in CASES:
        try:
            value = function(total)
            if type(value) is not int or not -(2**63) <= value < 2**63:
                raise TypeError("shipping_cost must return a bounded integer")
            values.append({"value": value, "error": None})
        except BaseException as error:
            values.append(error_result(error))
    return values


if __name__ == "__main__":
    before, after = measure("before"), measure("after")
    print(json.dumps({"cases": [
        {"total_cents": total, "expected_cents": expected, "before": old, "after": new}
        for (total, expected), old, new in zip(CASES, before, after, strict=True)]}))
