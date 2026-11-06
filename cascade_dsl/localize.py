"""Swap production connectors for local stand-ins with the same schema and changelog semantics.

Modes:
  plan     planning only (verify, describe, compile-plan smoke tests):
             append source -> datagen, changelog source -> empty filesystem debezium-json
             sinks -> blackhole
  restore  long-running local job for savepoint tests:
             append source -> datagen (slow, low-cardinality strings so keyed state builds up)
             changelog source -> continuously monitored filesystem debezium-json
             sinks -> blackhole
  test     bounded fixture data:
             every source -> filesystem file written from fixture rows (json / debezium-json)
             sinks, quarantine tables and INSERTs are removed (tests query views directly)
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal

from .analyze import Checked
from .lower import Lowered, Stmt

STRING_TYPES = ("STRING", "VARCHAR", "CHAR")


class FixtureError(ValueError):
    pass


def localize(lowered: Lowered, c: Checked, mode: str, workdir: str,
             fixtures: dict[str, list[dict]] | None = None, rows_per_second: int = 20) -> list[Stmt]:
    p = c.pipeline
    out: list[Stmt] = []
    for st in lowered.statements:
        if st.kind == "source":
            updating = p.relations[st.name].changelog == "updating"
            out.append(st.with_options(_source_options(st, updating, mode, workdir, rows_per_second,
                                                       (fixtures or {}).get(st.name), c)))
        elif st.kind in ("sink", "quarantine"):
            if mode != "test":
                out.append(st.with_options({"connector": "blackhole"}))
        elif st.kind == "insert":
            if mode != "test":
                out.append(st)
        else:
            out.append(st)
    return out


def _source_options(st: Stmt, updating: bool, mode: str, workdir: str, rps: int, rows, c: Checked) -> dict:
    path = os.path.join(workdir, "sources", st.name)
    if mode == "plan":
        if updating:
            os.makedirs(path, exist_ok=True)
            return {"connector": "filesystem", "path": path, "format": "debezium-json"}
        return {"connector": "datagen"}
    if mode == "restore":
        if updating:
            os.makedirs(path, exist_ok=True)
            return {"connector": "filesystem", "path": path, "format": "debezium-json",
                    "source.monitor-interval": "1s"}
        opts = {"connector": "datagen", "rows-per-second": str(rps)}
        for name, typ in st.columns:
            if typ.split("(")[0].split(" ")[0] in STRING_TYPES:
                opts[f"fields.{name}.length"] = "1"
        return opts
    # test
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "data.json"), "w") as f:
        for line in _fixture_lines(st, updating, rows or [], c):
            f.write(line + "\n")
    return {"connector": "filesystem", "path": path,
            "format": "debezium-json" if updating else "json"}


def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v) if v != v.to_integral() else int(v)
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(v, date):
        return v.isoformat()
    return v


def _fixture_lines(st: Stmt, updating: bool, rows: list[dict], c: Checked) -> list[str]:
    """Rows -> JSON lines. Changelog sources take `_op: insert | update | delete` (default insert);
    updates/deletes are matched to earlier rows by primary key to produce Debezium before-images."""
    names = [n for n, _ in st.columns]
    src = c.pipeline.sources[st.name]
    lines = []
    state: dict[tuple, dict] = {}
    for i, raw in enumerate(rows):
        if not isinstance(raw, dict):
            raise FixtureError(f"fixture row {i + 1} for '{st.name}' must be a mapping")
        row = dict(raw)
        op = row.pop("_op", "insert")
        unknown = [k for k in row if k not in names]
        if unknown:
            raise FixtureError(f"fixture row {i + 1} for '{st.name}' has unknown column(s) {unknown}; "
                               f"columns are {names}")
        full = {n: _jsonable(row.get(n)) for n in names}
        if not updating:
            if op != "insert":
                raise FixtureError(f"source '{st.name}' is append-only; `_op: {op}` is not allowed")
            lines.append(json.dumps(full))
            continue
        key = tuple(full.get(k) for k in src.primary_key)
        if op == "insert":
            state[key] = full
            lines.append(json.dumps({"before": None, "after": full, "op": "c"}))
        elif op == "update":
            before = state.get(key)
            if before is None:
                raise FixtureError(f"fixture row {i + 1} for '{st.name}': update of unknown key {key}")
            merged = {**before, **{k: _jsonable(v) for k, v in row.items()}}
            state[key] = merged
            lines.append(json.dumps({"before": before, "after": merged, "op": "u"}))
        elif op == "delete":
            before = state.pop(key, None)
            if before is None:
                raise FixtureError(f"fixture row {i + 1} for '{st.name}': delete of unknown key {key}")
            lines.append(json.dumps({"before": before, "after": None, "op": "d"}))
        else:
            raise FixtureError(f"fixture row {i + 1} for '{st.name}': _op must be insert, update or delete")
    return lines
