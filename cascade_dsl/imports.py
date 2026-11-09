"""Pipelines reading pipelines: a source declared with `from_pipeline` reads another project's sink.

    sources:
      ltv:
        from_pipeline: ../shop        # the upstream project directory
        sink: ltv_topic               # its sink
        startup: earliest             # optional: any source key overrides what is derived

The upstream project is compiled (same environment) and the source takes the sink's connection,
topic/table/path, columns, primary key and format. An upsert Kafka sink is read as an upsert-kafka
changelog; an exactly-once Kafka sink is read with isolation.level=read_committed. Re-resolved on
every compile, so a change to the upstream sink shows up as a schema change here.
"""
from __future__ import annotations

import copy
import os

from .connectors import spec_for
from .diagnostics import Diagnostics
from .ir import Column, Pipeline
from .types import parse_type

# sinks whose data can be streamed back (in streaming mode) as a source
STREAMABLE = {"kafka", "filesystem", "paimon", "hudi", "kinesis"}


def apply(p: Pipeline, d: Diagnostics, env: str | None, stack: tuple = ()) -> None:
    from .compiler import compile_pipeline
    here = os.path.realpath(p.root_dir)
    for s in list(p.sources.values()):
        imp = getattr(s, "from_pipeline", None)
        if not imp:
            continue
        path = os.path.realpath(os.path.join(p.root_dir, str(imp["path"])))
        if path in stack + (here,):
            d.error("CD130", f"source '{s.name}': pipelines import each other in a cycle "
                    f"({' -> '.join(os.path.basename(x) for x in stack + (here, path))})", s.loc, node=s.name)
            continue
        if not os.path.exists(os.path.join(path, "pipeline.yaml")):
            d.error("CD131", f"source '{s.name}': no pipeline.yaml in {imp['path']}", s.loc, node=s.name)
            continue
        up = compile_pipeline(path, env, emit=False, _imports=stack + (here,))
        if up.checked is None or up.diagnostics.has_errors:
            first = next((x.format() for x in up.diagnostics.errors), "")
            d.error("CD131", f"source '{s.name}': upstream pipeline {imp['path']} does not compile: {first}",
                    s.loc, node=s.name)
            continue
        q = up.checked.pipeline
        sink = q.sinks.get(str(imp.get("sink", "")))
        if sink is None:
            d.error("CD132", f"source '{s.name}': pipeline '{q.name}' has no sink '{imp.get('sink')}'", s.loc,
                    node=s.name, hint=f"its sinks: {', '.join(sorted(q.sinks)) or '(none)'}")
            continue
        conn = q.connections.get(sink.connection)
        spec = spec_for(conn.type, conn) if conn else None
        if conn is None or spec is None or "source" not in spec.roles:
            d.error("CD133", f"source '{s.name}': sink '{sink.name}' of '{q.name}' writes to "
                    f"'{conn.type if conn else sink.connection}', which can't be read back as a source",
                    s.loc, node=s.name)
            continue
        if p.mode == "streaming" and conn.type not in STREAMABLE and conn.type != "custom":
            d.error("CD134", f"source '{s.name}': sink '{sink.name}' of '{q.name}' writes to {conn.type}, which "
                    "can't be read as a stream", s.loc, node=s.name,
                    hint="read a Kafka/Kinesis/lakehouse sink of that pipeline, or use mode: batch")
            continue
        if conn.type == "iceberg" and sink.mode == "upsert":
            d.error("CD134", f"source '{s.name}': Iceberg's streaming read skips the equality deletes an upsert "
                    "sink writes", s.loc, node=s.name)
            continue

        # the connection: reuse an identical one, else bring it in under a generated name
        name = next((n for n, c in p.connections.items() if c.type == conn.type and c.properties == conn.properties
                     and c.custom == conn.custom), None)
        if name is None:
            name = f"{conn.name}__{q.name}"
            new = copy.deepcopy(conn)
            new.name = name
            p.connections[name] = new
            p.secrets |= {x for x in q.secrets}
        s.connection = name

        rel = q.relations[sink.from_]
        s.columns = [Column(c.name, (c.type.with_nullable(False) if c.name in sink.primary_key
                                     else c.type.with_nullable(True)), s.loc) for c in rel.columns]
        s.primary_key = s.primary_key or list(sink.primary_key)
        s.format = s.format or sink.format
        derived = {k: v for k, v in sink.endpoint.items() if k in spec.source}
        if conn.type == "kafka" and sink.mode == "upsert":
            derived["changelog"] = "upsert"
        s.endpoint = {**derived, **s.endpoint}
        sink_opts = up.checked.options.get(sink.name, {})
        if sink_opts.get("sink.delivery-guarantee") == "exactly-once":
            s.options.setdefault("properties.isolation.level", "read_committed")
        p.imports.append({"source": s.name, "pipeline": q.name, "sink": sink.name,
                          "path": os.path.relpath(path, p.root_dir)})
