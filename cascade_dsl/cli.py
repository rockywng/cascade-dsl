"""cascade — the cascade-dsl compiler CLI."""
from __future__ import annotations

import argparse
import json
import os
import sys

from . import __version__
from .compiler import compile_pipeline
from .connectors import CATALOG
from .diagnostics import Severity


def _print_diags(result, verbose: bool, as_json: bool) -> None:
    items = [d for d in result.diagnostics.items if verbose or d.severity != Severity.INFO]
    if as_json:
        print(json.dumps([d.to_json() for d in items], indent=2))
        return
    for d in items:
        print(d.format(), file=sys.stderr)
    errs = len(result.diagnostics.errors)
    warns = sum(1 for d in result.diagnostics.items if d.severity == Severity.WARNING)
    print(f"{errs} error(s), {warns} warning(s)", file=sys.stderr)


def _failed(result, strict: bool) -> bool:
    if result.diagnostics.has_errors:
        return True
    return strict and any(d.severity == Severity.WARNING for d in result.diagnostics.items)


def cmd_validate(args) -> int:
    r = compile_pipeline(args.path, args.env, emit=False)
    _print_diags(r, args.verbose, args.json)
    return 1 if _failed(r, args.strict) else 0


def cmd_compile(args) -> int:
    r = compile_pipeline(args.path, args.env)
    _print_diags(r, args.verbose, False)
    if _failed(r, args.strict) or r.lowered is None:
        return 1
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "pipeline.sql"), "w") as f:
        f.write(r.lowered.sql)
    with open(os.path.join(args.out, "manifest.json"), "w") as f:
        json.dump(r.lowered.manifest, f, indent=2)
        f.write("\n")
    print(f"wrote {args.out}/pipeline.sql and {args.out}/manifest.json", file=sys.stderr)
    return 0


def cmd_graph(args) -> int:
    from .lower import mermaid
    r = compile_pipeline(args.path, args.env, emit=False)
    if r.checked is None or r.diagnostics.has_errors:
        _print_diags(r, False, False)
        return 1
    print(mermaid(r.checked), end="")
    return 0


def cmd_verify(args) -> int:
    from .verify import find_flink_home, run_verify
    home = find_flink_home(args.flink_home)
    if home is None:
        print("error: Flink not found; pass --flink-home or set FLINK_HOME", file=sys.stderr)
        return 2
    r = compile_pipeline(args.path, args.env)
    if r.lowered is None:
        _print_diags(r, False, False)
        return 1
    print(f"planning with {home} ...", file=sys.stderr)
    v = run_verify(r.checked, r.lowered.sql, home)
    if v.error:
        print("error: Flink planner rejected the generated SQL:\n" + v.error, file=sys.stderr)
        return 1
    for name, mode in sorted(v.sink_modes.items()):
        print(f"  sink {name:<30} changelog [{mode}]")
    print(f"  planner operators: {', '.join(sorted(set(v.planner_ops))) or '(none)'}")
    for f in v.findings:
        print(f"  note: {f}")
    if args.plan:
        print(v.plan)
    print("verify: OK" if v.ok else "verify: FAILED", file=sys.stderr)
    return 0 if v.ok else 1


def cmd_check_upgrade(args) -> int:
    from .upgrade import diff_manifests
    with open(args.old) as f:
        old = json.load(f)
    if args.new.endswith(".json"):
        with open(args.new) as f:
            new = json.load(f)
    else:
        r = compile_pipeline(args.new, args.env)
        if r.lowered is None:
            _print_diags(r, False, False)
            return 1
        new = r.lowered.manifest
    verdict, changes = diff_manifests(old, new)
    for c in changes:
        print(f"  [{c.level:<10}] {c.code:<22} {c.message}")
    print(f"upgrade verdict: {verdict}")
    if verdict == "breaking":
        return 1
    if verdict == "state_loss" and not args.allow_state_loss:
        print("refusing: this upgrade discards state (re-run with --allow-state-loss to accept)", file=sys.stderr)
        return 3
    return 0


def cmd_connectors(args) -> int:
    for spec in sorted(CATALOG.values(), key=lambda s: s.type):
        print(f"{spec.type}  [{', '.join(sorted(spec.roles))}]  {spec.doc}")
        if spec.connection:
            print("    connection: " + ", ".join(k + ("*" if p.required else "") for k, p in spec.connection.items()))
        if "source" in spec.roles and spec.source:
            print("    source:     " + ", ".join(k + ("*" if p.required else "") for k, p in spec.source.items()))
        if "sink" in spec.roles:
            keys = ", ".join(k + ("*" if p.required else "") for k, p in spec.sink.items())
            print(f"    sink:       {keys or '-'}  modes: {', '.join(sorted(spec.sink_modes))}")
        if spec.formats:
            print("    formats:    " + ", ".join(sorted(spec.formats)))
    print("\n(* = required)")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="cascade", description="Compile cascade-dsl pipelines to Flink SQL")
    ap.add_argument("--version", action="version", version=f"cascade {__version__}")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("path", help="project directory or pipeline.yaml")
        sp.add_argument("--env", help="environment to resolve vars for (see `environments:`)")

    sp = sub.add_parser("validate", help="check a pipeline without generating output")
    common(sp)
    sp.add_argument("--json", action="store_true", help="machine-readable diagnostics")
    sp.add_argument("--strict", action="store_true", help="treat warnings as errors")
    sp.add_argument("-v", "--verbose", action="store_true", help="also show info diagnostics")
    sp.set_defaults(fn=cmd_validate)

    sp = sub.add_parser("compile", help="generate Flink SQL and the deployment manifest")
    common(sp)
    sp.add_argument("-o", "--out", default="target")
    sp.add_argument("--strict", action="store_true")
    sp.add_argument("-v", "--verbose", action="store_true")
    sp.set_defaults(fn=cmd_compile)

    sp = sub.add_parser("graph", help="print the pipeline DAG as a Mermaid flowchart")
    common(sp)
    sp.set_defaults(fn=cmd_graph)

    sp = sub.add_parser("verify", help="plan the generated SQL with a local Flink and cross-check it")
    common(sp)
    sp.add_argument("--flink-home")
    sp.add_argument("--plan", action="store_true", help="print the full optimized plan")
    sp.set_defaults(fn=cmd_verify)

    sp = sub.add_parser("check-upgrade", help="is the new version savepoint-compatible with the old one?")
    sp.add_argument("old", help="manifest.json of the running version")
    sp.add_argument("new", help="new project directory, pipeline.yaml, or manifest.json")
    sp.add_argument("--env")
    sp.add_argument("--allow-state-loss", action="store_true")
    sp.set_defaults(fn=cmd_check_upgrade)

    sp = sub.add_parser("connectors", help="list available connectors and their properties")
    sp.set_defaults(fn=cmd_connectors)

    args = ap.parse_args(argv)
    return args.fn(args)
