"""Swap production connectors for local stand-ins with the same schema and changelog semantics.

Modes:
  plan     planning only (verify, describe, compile-plan smoke tests):
             append source -> datagen, changelog source -> empty filesystem debezium-json
             sinks -> blackhole
  restore  long-running local job for savepoint tests:
             append source -> datagen (slow, low-cardinality strings so keyed state builds up)
             changelog source -> continuously monitored filesystem debezium-json
             sinks -> blackhole
  test     bounded fixture data, via the tools jar's `cascade-fixture` connector:
             every source -> fixture file of changelog rows and explicit watermarks
               (WATERMARK ... AS SOURCE_WATERMARK(), so the fixture decides when time advances);
               also serves lookup joins from the same rows
             sinks, quarantine tables and INSERTs are removed (tests query views directly),
             except a sink under test, which captures every change it receives
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal

from .analyze import Checked
from .lower import Lowered, Stmt

STRING_TYPES = ("STRING", "VARCHAR", "CHAR")


class FixtureError(ValueError):
    pass


WATERMARK_MODES = ("per_row", "manual", "end_of_input")


def localize(lowered: Lowered, c: Checked, mode: str, workdir: str,
             fixtures: dict[str, list[dict]] | None = None, rows_per_second: int = 20,
             watermarks: str = "per_row", capture: tuple[str, str] | None = None) -> list[Stmt]:
    """`capture` = (sink name, file): in test mode keep that sink, recording what it receives."""
    p = c.pipeline
    out: list[Stmt] = []
    py = _python_executable() if any(f.language == "python" for f in p.functions.values()) else None
    for st in lowered.statements:
        if mode == "test" and st.kind == "ensure":
            continue  # fixtures stand in for the table
        if mode == "test" and st.kind == "set" and st.name == "execution.runtime-mode":
            continue  # fixtures are streams; for bounded inputs a streaming run ends with the same results
        if py and st.kind == "set" and st.name == "python.files":
            # local runs: PyFlink workers use an interpreter that has pyflink installed
            for key in ("python.executable", "python.client.executable"):
                out.append(Stmt("set", key, f"SET '{key}' = '{py}';", options={key: py}))
        if st.kind == "source":
            updating = p.relations[st.name].changelog == "updating"
            if mode == "test":
                rows = (fixtures or {}).get(st.name)
                if rows is None and p.sources[st.name].copy_of:  # backfill copies replay the same input
                    rows = (fixtures or {}).get(p.sources[st.name].copy_of)
                out.append(_fixture_source(st, updating, workdir, rows, c, watermarks))
                continue
            out.append(st.with_options(_source_options(st, updating, mode, workdir, rows_per_second, c)))
        elif st.kind in ("sink", "quarantine"):
            if mode != "test":
                out.append(st.with_options({"connector": "blackhole"}))
            elif capture and st.name == capture[0]:
                sink = p.sinks[st.name]
                out.append(st.with_options({"connector": "cascade-fixture", "path": capture[1],
                                            "mode": sink.mode}))
        elif st.kind == "insert":
            if mode != "test" or (capture and st.name == capture[0]):
                out.append(st)
        else:
            out.append(st)
    return out


def _python_executable() -> str | None:
    from .jvm import python_executable
    return python_executable()


def _source_options(st: Stmt, updating: bool, mode: str, workdir: str, rps: int, c: Checked) -> dict:
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
    raise ValueError(f"unknown mode {mode}")


def _fixture_source(st: Stmt, updating: bool, workdir: str, rows, c: Checked, watermarks: str) -> Stmt:
    os.makedirs(os.path.join(workdir, "sources"), exist_ok=True)
    path = os.path.join(workdir, "sources", f"{st.name}.jsonl")
    with open(path, "w") as f:
        for line in _fixture_lines(st, updating, rows or [], c, watermarks):
            f.write(line + "\n")
    src = c.pipeline.sources[st.name]
    extra = [x for x in st.extra if not x.startswith("WATERMARK FOR")]
    if src.event_time:
        col = "`" + src.event_time.column + "`"
        extra.append(f"WATERMARK FOR {col} AS SOURCE_WATERMARK()")
    return Stmt(st.kind, st.name, st.sql, st.comment, st.columns, extra,
                {"connector": "cascade-fixture", "path": path, "changelog": "all" if updating else "insert-only"})


def _epoch_ms(v) -> int | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        text = str(v).strip().replace("T", " ").replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            raise FixtureError(f"cannot read '{v}' as a timestamp")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _jsonable(v):
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(v, date):
        return v.isoformat()
    return v


def _fixture_lines(st: Stmt, updating: bool, rows: list[dict], c: Checked, watermarks: str) -> list[str]:
    """Rows -> changelog JSON lines for the fixture source. Changelog sources take
    `_op: insert | update | delete` (default insert); updates/deletes are matched to earlier rows by
    primary key to produce before-images. `{_watermark: <timestamp>}` emits a watermark at that point.

    watermarks: per_row       after each row, max event time so far minus the declared delay
                              (the source's WATERMARK strategy, applied deterministically)
                manual        only explicit `_watermark` rows
                end_of_input  none until all input is read (every window fires at the end)"""
    names = [n for n, _ in st.columns]
    src = c.pipeline.sources[st.name]
    et = src.event_time
    lines: list[str] = []
    state: dict[tuple, dict] = {}
    max_ts: int | None = None
    last_wm: int | None = None

    def emit(kind, row):
        lines.append(json.dumps({"kind": kind, "values": [row.get(n) for n in names]}))

    def advance(row):
        nonlocal max_ts, last_wm
        if et is None or watermarks != "per_row":
            return
        ts = _epoch_ms(row.get(et.column))
        if ts is None:
            return
        max_ts = ts if max_ts is None else max(max_ts, ts)
        wm = max_ts - et.max_out_of_orderness_ms
        if last_wm is None or wm > last_wm:
            last_wm = wm
            lines.append(json.dumps({"watermark": wm}))

    for i, raw in enumerate(rows):
        if not isinstance(raw, dict):
            raise FixtureError(f"fixture row {i + 1} for '{st.name}' must be a mapping")
        row = dict(raw)
        if "_watermark" in row:
            if len(row) != 1:
                raise FixtureError(f"fixture row {i + 1} for '{st.name}': `_watermark` must be alone in its row")
            if et is None:
                raise FixtureError(f"source '{st.name}' has no event_time; `_watermark` does not apply")
            wm = _epoch_ms(row["_watermark"])
            last_wm = wm if last_wm is None else max(last_wm, wm)
            lines.append(json.dumps({"watermark": wm}))
            continue
        op = row.pop("_op", "insert")
        unknown = [k for k in row if k not in names]
        if unknown:
            raise FixtureError(f"fixture row {i + 1} for '{st.name}' has unknown column(s) {unknown}; "
                               f"columns are {names}")
        full = {n: _jsonable(row.get(n)) for n in names}
        if not updating:
            if op != "insert":
                raise FixtureError(f"source '{st.name}' is append-only; `_op: {op}` is not allowed")
            emit("+I", full)
            advance(full)
            continue
        key = tuple(full.get(k) for k in src.primary_key)
        if op == "insert":
            state[key] = full
            emit("+I", full)
            advance(full)
        elif op == "update":
            before = state.get(key)
            if before is None:
                raise FixtureError(f"fixture row {i + 1} for '{st.name}': update of unknown key {key}")
            merged = {**before, **{k: _jsonable(v) for k, v in row.items()}}
            state[key] = merged
            emit("-U", before)
            emit("+U", merged)
            advance(merged)
        elif op == "delete":
            before = state.pop(key, None)
            if before is None:
                raise FixtureError(f"fixture row {i + 1} for '{st.name}': delete of unknown key {key}")
            emit("-D", before)
        else:
            raise FixtureError(f"fixture row {i + 1} for '{st.name}': _op must be insert, update or delete")
    return lines
