"""`cascade test`: run models on fixture rows in a local Flink and compare the results.

Test files live in the project's tests/ directory (*.yaml):

    tests:
      - name: ltv_sums_amounts_per_customer
        target: customer_ltv              # a model, or <model>.quarantined
        given:                            # rows per source; unlisted sources are empty
          orders:
            - {order_id: o1, customer_id: c1, amount: 10.00, currency: usd, order_ts: "2026-01-01 10:00:00"}
          customers:                      # changelog sources accept _op: insert|update|delete
            - {customer_id: c1, segment: smb, updated_at: "2026-01-01 09:00:00"}
        expect:
          rows:                           # final table after applying the changelog;
            - {customer_id: c1, lifetime_value: 10.00}   # only listed columns are compared
          # or: error: positive_amount    (the job must fail, message containing this text)
        ordered: false                    # default: compare as a multiset

Sources are bounded, so event time advances to the end of input and every window fires.
"""
from __future__ import annotations

import glob
import os
import tempfile
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

import yaml

from .compiler import compile_pipeline
from .diagnostics import Location
from .jvm import Tools
from .loader import _Loader
from .localize import FixtureError, localize


@dataclass
class TestCase:
    name: str
    target: str
    given: dict
    rows: list | None
    error: str | None
    ordered: bool
    loc: Location


@dataclass
class TestResult:
    case: TestCase
    passed: bool
    message: str = ""
    actual: list = field(default_factory=list)
    millis: int = 0


class TestSpecError(ValueError):
    pass


def discover(root: str, pattern: str | None = None) -> list[TestCase]:
    cases = []
    for path in sorted(glob.glob(os.path.join(root, "tests", "*.yaml")) +
                       glob.glob(os.path.join(root, "tests", "*.yml"))):
        rel = os.path.relpath(path)
        with open(path) as f:
            doc = yaml.load(f, Loader=_Loader) or {}
        for t in doc.get("tests", []) or []:
            loc = Location(rel, getattr(t, "line", None))
            if not isinstance(t, dict) or "name" not in t or "target" not in t:
                raise TestSpecError(f"{loc}: each test needs `name` and `target`")
            unknown = set(t) - {"name", "target", "given", "expect", "ordered", "description"}
            if unknown:
                raise TestSpecError(f"{loc}: unknown key(s) {sorted(unknown)} in test '{t['name']}'")
            exp = t.get("expect") or {}
            if not isinstance(exp, dict) or not ({"rows", "error", "empty"} & set(exp)):
                raise TestSpecError(f"{loc}: test '{t['name']}' needs expect.rows, expect.empty or expect.error")
            rows = [] if exp.get("empty") else exp.get("rows")
            if pattern and pattern not in t["name"]:
                continue
            cases.append(TestCase(t["name"], t["target"], dict(t.get("given") or {}), rows,
                                  exp.get("error"), bool(t.get("ordered", False)), loc))
    return cases


def run(path: str, flink_home: str, env: str | None = None, pattern: str | None = None):
    """Returns (compile result, [TestResult]) — results is None if compilation failed."""
    r = compile_pipeline(path, env)
    if r.lowered is None:
        return r, None
    c = r.checked
    p = c.pipeline
    cases = discover(p.root_dir, pattern)
    results: list[TestResult] = []
    runnable = []
    with tempfile.TemporaryDirectory(prefix="cascade-test-") as work:
        for i, case in enumerate(cases):
            problem = _validate_case(case, p)
            if problem:
                results.append(TestResult(case, False, problem))
                continue
            try:
                stmts = localize(r.lowered, c, "test", os.path.join(work, f"t{i}"), fixtures=case.given)
            except FixtureError as e:
                results.append(TestResult(case, False, f"bad fixture: {e}"))
                continue
            view = case.target.replace(".quarantined", "__quarantined")
            runnable.append((case, stmts, f"SELECT * FROM `{view}`"))
        if runnable:
            outputs = Tools(flink_home).run_tests([(cs.name, st, qy) for cs, st, qy in runnable])
            for (case, _, _), out in zip(runnable, outputs):
                results.append(_evaluate(case, out))
    order = {id(cs): i for i, cs in enumerate(cases)}
    results.sort(key=lambda x: order[id(x.case)])
    return r, results


def _validate_case(case: TestCase, p) -> str | None:
    target = case.target
    if target.endswith(".quarantined"):
        m = target[: -len(".quarantined")]
        if m not in p.models or not any(e.on_violation == "quarantine" for e in p.models[m].expectations):
            return f"target '{target}': '{m}' is not a model with quarantine expectations"
    elif target not in p.models and target not in p.sources:
        return f"unknown target '{target}'"
    for src in case.given:
        if src not in p.sources:
            return f"`given` names '{src}', which is not a source"
        if not isinstance(case.given[src], list):
            return f"`given.{src}` must be a list of rows"
    return None


# --- evaluation ---------------------------------------------------------------------------

def _materialize(rows: list[dict]) -> list[tuple]:
    """Apply a changelog (+I, -U, +U, -D) and return the final rows."""
    state: Counter = Counter()
    for r in rows:
        values = tuple(_freeze(v) for v in r["values"])
        if r["kind"] in ("+I", "+U"):
            state[values] += 1
        elif state[values] > 0:
            state[values] -= 1
    return [row for row, n in state.items() for _ in range(n)]


def _freeze(v):
    if isinstance(v, list):
        return tuple(_freeze(x) for x in v)
    if isinstance(v, dict):
        return tuple(sorted((k, _freeze(x)) for k, x in v.items()))
    return v


def _parse_ts(v):
    if isinstance(v, datetime):
        return v.replace(tzinfo=None) if v.tzinfo is None else v.astimezone(timezone.utc).replace(tzinfo=None)
    s = str(v).strip().replace("T", " ").replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt


def normalize(value, flink_type: str):
    """Normalize expected and actual values to comparable Python values by column type."""
    if value is None:
        return None
    base = flink_type.split("(")[0].split(" ")[0].split("<")[0]
    try:
        if base == "DECIMAL":
            return Decimal(str(value)).normalize()
        if base in ("FLOAT", "DOUBLE"):
            return round(float(value), 9)
        if base in ("TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT"):
            return int(value)
        if base == "BOOLEAN":
            return value if isinstance(value, bool) else str(value).lower() == "true"
        if base in ("TIMESTAMP", "TIMESTAMP_LTZ"):
            return _parse_ts(value)
        if base == "DATE":
            return value.isoformat() if isinstance(value, date) else str(value)
    except (InvalidOperation, ValueError, TypeError):
        return ("<unparseable>", str(value))
    if base in ("CHAR", "VARCHAR", "STRING"):
        return str(value).rstrip() if base == "CHAR" else str(value)
    return _freeze(value)


def _evaluate(case: TestCase, out: dict) -> TestResult:
    millis = out.get("millis", 0)
    if out.get("error"):
        if case.error is not None:
            if case.error in out["error"]:
                return TestResult(case, True, "failed as expected", millis=millis)
            return TestResult(case, False, f"failed, but the error does not mention '{case.error}':\n"
                              f"{_short(out['error'])}", millis=millis)
        return TestResult(case, False, f"job failed:\n{_short(out['error'])}", millis=millis)
    if case.error is not None:
        return TestResult(case, False, f"expected a failure mentioning '{case.error}', but the job succeeded",
                          millis=millis)

    columns = [(col["name"], col["type"]) for col in out["columns"]]
    types = dict(columns)
    names = [n for n, _ in columns]
    final = _materialize(out["rows"])
    actual_dicts = [dict(zip(names, row)) for row in final]

    compare_cols = []
    for row in case.rows:
        for k in row:
            if k not in types:
                return TestResult(case, False, f"expected column '{k}' is not an output column of "
                                  f"'{case.target}' (columns: {', '.join(names)})", actual_dicts, millis)
            if k not in compare_cols:
                compare_cols.append(k)
    if not compare_cols:
        compare_cols = names

    def key(row: dict) -> tuple:
        return tuple(normalize(row.get(k), types[k]) for k in compare_cols)

    exp_keys = [key(r) for r in case.rows]
    act_keys = [key(r) for r in actual_dicts]
    if case.ordered:
        ok = exp_keys == act_keys
        missing = [k for k in exp_keys if k not in act_keys]
        extra = [k for k in act_keys if k not in exp_keys]
    else:
        e, a = Counter(exp_keys), Counter(act_keys)
        ok = e == a
        missing = list((e - a).elements())
        extra = list((a - e).elements())
    if ok:
        return TestResult(case, True, f"{len(final)} row(s)", actual_dicts, millis)
    lines = [f"compared columns: {', '.join(compare_cols)}"]
    for label, rows in (("missing (expected, not produced)", missing), ("unexpected (produced, not expected)", extra)):
        if rows:
            lines.append(f"{label}:")
            lines += [f"  - {_fmt(dict(zip(compare_cols, r)))}" for r in rows[:20]]
    if case.ordered and not missing and not extra:
        lines.append("same rows, different order")
    return TestResult(case, False, "\n".join(lines), actual_dicts, millis)


def _fmt(row: dict) -> str:
    def f(v):
        if isinstance(v, datetime):
            return v.isoformat(sep=" ")
        if isinstance(v, Decimal):
            return format(v, "f")
        return repr(v) if isinstance(v, str) else str(v)
    return "{" + ", ".join(f"{k}: {f(v)}" for k, v in row.items()) + "}"


def _short(err: str, lines: int = 6) -> str:
    parts = err.split("\n")
    # the most specific cause is usually last
    keep = parts[:1] + (parts[-(lines - 1):] if len(parts) > lines else parts[1:])
    return "\n".join("    " + x.strip() for x in keep)
