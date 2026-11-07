"""`backfill: true` on a model: build its state from history, independently of the running job.

Adding a stateful model to a running pipeline has a problem no savepoint can solve: its operators
start empty, while the sources resume from their saved positions, so the model only ever sees
post-upgrade input. Worse, the planner may give the new model its own copy of an *existing*
operator (a temporal join, say) that also starts empty.

With `backfill: true`, the model and everything upstream of it are compiled over dedicated copies
of their sources, named `<source>__bf_<model>`, that start from the beginning:
  - Kafka: from the earliest offset, with its own consumer group
  - CDC: with a fresh snapshot and its own replication slot
Upstream models are copied as `<model>__bf_<backfilled model>`, with their expectations; a
quarantine becomes a drop, so quarantined rows are not written twice. The rest of the job is
untouched and keeps its state.

The copies are part of the job from then on. Removing `backfill` later makes the model read the
shared sources again, which is a state-incompatible change, and check-upgrade reports it.
Limits: history is what the sources still hold (Kafka retention), and a CDC snapshot has only the
current version of each row. An event-time temporal join over it finds no version for events
older than a row's last change.
"""
from __future__ import annotations

import copy
import re

from .connectors import CATALOG
from .diagnostics import Diagnostics
from .ir import Expectation, Pipeline
from .sqlscan import SqlSyntaxError, analyze_sql

NO_COPY = {"jdbc", "datagen"}  # lookup tables and generators: nothing to replay


def suffix(model: str) -> str:
    return f"__bf_{model}"


def _refs(sql: str):
    try:
        return analyze_sql(sql)[1].refs
    except SqlSyntaxError:
        return []


def _rewrite(sql: str, mapping: dict[str, str]) -> str:
    for r in sorted(_refs(sql), key=lambda r: -r.pos):
        if r.name in mapping:
            m = re.compile(r"`?" + re.escape(r.name) + r"`?", re.IGNORECASE).match(sql, r.pos)
            if m:
                sql = sql[:m.start()] + mapping[r.name] + sql[m.end():]
    return sql


def _connector_type(p: Pipeline, src) -> str | None:
    if src.connection in p.connections:
        return p.connections[src.connection].type
    return src.connection if src.connection in CATALOG else None


def apply(p: Pipeline, diags: Diagnostics) -> None:
    """Rewrite the pipeline in place (before analysis)."""
    for m in [m for m in p.models.values() if m.backfill]:
        sfx = suffix(m.name)
        models, sources, stack = [], [], [m.name]
        seen = set()
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            sql = p.models[n].sql
            for r in _refs(sql):
                if r.name in p.models and r.name not in seen:
                    models.append(r.name)
                    stack.append(r.name)
                elif r.name in p.sources and r.name not in sources:
                    sources.append(r.name)
        # event-time temporal joins over CDC in the backfilled branch: with an idle timeout, the fact
        # history (instantly available) can outrun the CDC snapshot (which takes a while to start)
        cdc_versioned = sorted({r.name for n in seen for r in _refs(p.models[n].sql)
                                if r.temporal and r.name in p.sources
                                and (_connector_type(p, p.sources[r.name]) or "").endswith("-cdc")
                                and p.sources[r.name].event_time is not None})
        idle = p.flink_config.get("table.exec.source.idle-timeout")
        if not idle or idle in ("0", "0 ms", "0ms", "0s"):
            idle = next((f"{p.sources[s].event_time.idle_timeout_ms} ms" for s in cdc_versioned
                         if p.sources[s].event_time.idle_timeout_ms), None)
        if cdc_versioned and idle:
            diags.warning("CD372", f"model '{m.name}': its backfill replays history through an event-time "
                          f"temporal join on CDC source(s) {', '.join(cdc_versioned)}, and "
                          f"an idle timeout ({idle}) lets event time advance without them. "
                          "Historical rows can be joined before the CDC snapshot arrives and then find no "
                          "version (the outcome depends on timing)", m.loc, node=m.name,
                          hint="backfill models that don't need that join, or accept 'no match' for history; "
                               "rows arriving after the snapshot are joined correctly")
        order = list(p.sources) + list(p.models)  # report in declaration order
        sources.sort(key=order.index)
        models.sort(key=order.index)
        mapping: dict[str, str] = {}
        copied_sources = []
        for s in sources:
            src = p.sources[s]
            ctype = _connector_type(p, src)
            if ctype in NO_COPY:
                continue
            c = copy.deepcopy(src)
            c.name, c.copy_of = s + sfx, s
            ep = dict(c.endpoint)
            if ctype == "kafka":
                ep["startup"] = "earliest"
                if ep.get("group_id"):
                    ep["group_id"] = f"{ep['group_id']}.backfill.{m.name}"
            elif ctype in ("postgres-cdc", "mysql-cdc"):
                ep["startup"] = "initial"
                if ctype == "mysql-cdc" and ep.pop("server_id", None) is not None:
                    diags.warning("CD371", f"model '{m.name}': the backfill copy of '{s}' needs its own MySQL "
                                  "server-id; it was removed so Flink CDC picks one", m.loc, node=m.name)
            c.endpoint = ep
            p.sources[c.name] = c
            mapping[s] = c.name
            copied_sources.append(s)
        for u in models:
            mapping[u] = u + sfx
        for u in models:
            orig = p.models[u]
            c = copy.deepcopy(orig)
            c.name, c.copy_of, c.backfill, c.scaling = u + sfx, u, False, None
            c.sql = _rewrite(orig.sql, mapping)
            c.expectations = [Expectation(e.name, e.check, "drop" if e.on_violation == "quarantine"
                                          else e.on_violation, e.loc) for e in orig.expectations]
            c.description = f"backfill copy of '{u}' for '{m.name}'"
            p.models[c.name] = c
        m.sql = _rewrite(m.sql, mapping)
        diags.info("CD370", f"model '{m.name}' backfills: it reads its own copies of "
                   f"{', '.join(copied_sources) or 'no sources'} from the beginning"
                   + (f" and recomputes {', '.join(models)}" if models else ""), m.loc, node=m.name,
                   hint="history is limited to what the sources still retain; a CDC snapshot holds only "
                        "the current version of each row")
