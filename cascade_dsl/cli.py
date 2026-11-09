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
    r = compile_pipeline(args.path, args.env)
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
    r = compile_pipeline(args.path, args.env)
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
    v = run_verify(r.checked, r.lowered, home)
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


def _flink(args) -> str | None:
    from .jvm import find_flink_home
    home = find_flink_home(getattr(args, "flink_home", None))
    if home is None:
        print("error: Flink not found; pass --flink-home or set FLINK_HOME", file=sys.stderr)
    return home


def cmd_infer(args) -> int:
    from .schemas import InferError, infer
    home = _flink(args)
    if home is None:
        return 2
    try:
        doc, path = infer(args.path, home, args.env)
    except InferError as e:
        if e.result is not None:
            _print_diags(e.result, False, False)
        print(f"error: {e}", file=sys.stderr)
        return 1
    for name, entry in doc["models"].items():
        cols = ", ".join(f"{c['name']} {c['type']}" for c in entry["columns"])
        print(f"  {name}: {cols}")
    print(f"wrote {path}", file=sys.stderr)
    return 0


def cmd_test(args) -> int:
    from .testing import TestSpecError, run
    home = _flink(args)
    if home is None:
        return 2
    try:
        r, results = run(args.path, home, args.env, args.k)
    except TestSpecError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if results is None:
        _print_diags(r, False, False)
        return 1
    failed = 0
    for t in results:
        status = "PASS" if t.passed else "FAIL"
        failed += not t.passed
        print(f"{status}  {t.case.name}  ({t.case.target}, {t.millis} ms)")
        if not t.passed or args.verbose:
            print("      " + t.message.replace("\n", "\n      ") + f"\n      at {t.case.loc}")
    print(f"{len(results) - failed} passed, {failed} failed", file=sys.stderr)
    return 1 if failed or not results else 0


def _print_pin_report(report) -> None:
    print(f"  operators keeping their UIDs: {len(report.exact)} identical"
          + (f", {len(report.drift)} with changed upstream logic" if report.drift else ""))
    for new, old in report.drift:
        print(f"    kept despite upstream change: {new} (was {old})")
    for x in report.new_stateful:
        print(f"    new stateful operator (starts empty): {x}")
    for x in report.dropped_stateful:
        print(f"    state dropped (operator removed or changed): {x}")
    for x in report.dropped_sinks:
        print(f"    sink replaced: {x} (keeps no state unless it writes exactly-once transactions)")
    if report.needs_history:
        print("  WARNING: new operators that need data from before the upgrade (results will be incomplete "
              "until they have seen it; backfill or replay their inputs):")
        for label, why, sinks, restored in report.needs_history:
            print(f"    {label} (reads restored {', '.join(restored)}; feeds {', '.join(sinks) or '?'}): {why}")
        if report.suggest_backfill:
            print("  fix: build the new state from history with `backfill: true` on "
                  + ", ".join(f"model '{m}'" for m in report.suggest_backfill))


def cmd_plan(args) -> int:
    from .jvm import script
    from .plans import PlanError, compile_plan, operator_map_for
    home = _flink(args)
    if home is None:
        return 2
    os.makedirs(args.out, exist_ok=True)
    plan = os.path.join(args.out, "plan.json")
    try:
        r, report, setup = compile_plan(args.path, home, plan, args.env, local=args.local_connectors,
                                        previous=args.previous)
        ops = operator_map_for(r, setup, plan, home)
    except PlanError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    with open(os.path.join(args.out, "plan-setup.sql"), "w") as f:
        f.write("-- Run before: EXECUTE PLAN 'plan.json' (plans reference these by name)\n")
        f.write(script(setup))
    with open(os.path.join(args.out, "operators.json"), "w") as f:
        json.dump(ops, f, indent=2)
    with open(os.path.join(args.out, "restore.json"), "w") as f:
        # which sources resume from the previous version's savepoint (deploy keeps their restore guards)
        json.dump({"previous": args.previous, "restored_sources": report.restored_sources if report else []}, f,
                  indent=2)
    if report is not None:
        _print_pin_report(report)
    print("vertices:", file=sys.stderr)
    for v in ops["vertices"]:
        what = ", ".join(v["sources"] + v["models"] + v["sinks"]) or "?"
        print(f"  {v['id'][:8]}  x{v['parallelism']:<3} {what}", file=sys.stderr)
    if ops["fused_away"]:
        print(f"models with no operators of their own (fused into consumers): {', '.join(ops['fused_away'])}",
              file=sys.stderr)
    print(f"wrote {args.out}/plan.json, plan-setup.sql and operators.json", file=sys.stderr)
    return 0


def cmd_restore_test(args) -> int:
    from .plans import PlanError, restore_test
    home = _flink(args)
    if home is None:
        return 2
    print(f"running {args.old} locally, taking a savepoint, restoring {args.new} "
          f"({'with' if not args.no_pin else 'without'} UID pinning) ...", file=sys.stderr)
    try:
        res = restore_test(args.old, args.new, home, args.env, pin=not args.no_pin, seconds=args.seconds,
                           allow_state_loss=args.allow_state_loss)
    except PlanError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if res.get("report") is not None:
        _print_pin_report(res["report"])
    if res.get("restored"):
        print("restore: OK (all savepoint state was mapped to the new job)")
        return 0
    err = res.get("error", "").strip().splitlines()
    print("restore: FAILED\n  " + (err[-1].strip() if err else res.get("status", "?")))
    return 1


def cmd_deploy(args) -> int:
    from .deploy import DeployError, build, secret_key
    r = compile_pipeline(args.path, args.env)
    _print_diags(r, False, False)
    if not r.ok:
        return 1
    try:
        info = build(r, args.out, plan_dir=args.plan_dir, image=args.image)
    except DeployError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"wrote {info['out']}: FlinkDeployment '{info['name']}' ({info['mode']} mode)", file=sys.stderr)
    if info["mode"] == "statements":
        print("  note: without --plan-dir the job is planned at startup and operator UIDs are not pinned; "
              "use `cascade plan` for upgrades that must keep state", file=sys.stderr)
    if info["secrets"] and not info.get("secret_name"):
        from .deploy import env_name
        print("  needs these environment variables where the job runs: "
              + ", ".join(sorted(env_name(s) for s in info["secrets"])), file=sys.stderr)
    elif info["secrets"]:
        print(f"  needs Secret '{info['secret_name']}' with keys: "
              + ", ".join(sorted(secret_key(s) for s in info["secrets"])) + " (see secret.template.yaml)",
              file=sys.stderr)
    return 0


def cmd_import_schema(args) -> int:
    from .registry import SchemaImportError, avro_columns, columns_yaml, fetch
    try:
        if args.avsc:
            with open(args.avsc) as f:
                text = f.read()
        else:
            if not (args.registry and args.subject):
                print("error: pass --avsc FILE, or --registry URL and --subject NAME", file=sys.stderr)
                return 2
            kind, text = fetch(args.registry, args.subject, args.version)
            if kind != "AVRO":
                print(f"error: subject is {kind}; only AVRO schemas are supported", file=sys.stderr)
                return 1
        print(columns_yaml(avro_columns(text), args.source), end="")
    except (SchemaImportError, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


def cmd_fetch_jars(args) -> int:
    from .compiler import compile_pipeline
    from .deploy import DEFAULT_FLINK
    from .jars import FetchError, fetch
    r = compile_pipeline(args.path, args.env)
    if not r.ok:
        _print_diags(r, False, False)
        return 1
    deploy = r.checked.pipeline.deploy or {}
    flink = str(deploy.get("flink_version", DEFAULT_FLINK))
    try:
        paths = fetch(r.lowered.manifest["artifacts"], ".".join(flink.split(".")[:2]),
                      {str(k): str(v) for k, v in (deploy.get("artifacts") or {}).items()})
    except FetchError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    for p in paths:
        print(p, file=sys.stderr)
    print(f"export CASCADE_EXTRA_JARS={os.pathsep.join(paths)}")
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
    ap = argparse.ArgumentParser(prog="cascade", description="Compile, test and ship cascade-dsl pipelines on Flink")
    ap.add_argument("--version", action="version", version=f"cascade {__version__}")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("path", help="project directory, pipeline.yaml or pipeline.py")
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

    sp = sub.add_parser("infer", help="ask Flink for exact model schemas and write cascade.lock.json")
    common(sp)
    sp.add_argument("--flink-home")
    sp.set_defaults(fn=cmd_infer)

    sp = sub.add_parser("test", help="run tests/*.yaml against a local Flink")
    common(sp)
    sp.add_argument("--flink-home")
    sp.add_argument("-k", help="only run tests whose name contains this text")
    sp.add_argument("-v", "--verbose", action="store_true")
    sp.set_defaults(fn=cmd_test)

    sp = sub.add_parser("plan", help="compile the job to a Flink plan (EXECUTE PLAN), pinning UIDs to a previous plan")
    common(sp)
    sp.add_argument("--flink-home")
    sp.add_argument("-o", "--out", default="target")
    sp.add_argument("--previous", help="plan.json of the running version; unchanged operators keep their UIDs")
    sp.add_argument("--local-connectors", action="store_true",
                    help="use local stand-in connectors (when production connector jars are not on the classpath)")
    sp.set_defaults(fn=cmd_plan)

    sp = sub.add_parser("restore-test", help="run OLD locally, savepoint it, and restore NEW from the savepoint")
    sp.add_argument("old", help="project of the running version")
    sp.add_argument("new", help="project of the new version")
    sp.add_argument("--env")
    sp.add_argument("--flink-home")
    sp.add_argument("--no-pin", action="store_true", help="do not pin UIDs (shows what an unpinned deploy does)")
    sp.add_argument("--seconds", type=int, default=8, help="how long to run OLD before the savepoint")
    sp.add_argument("--allow-state-loss", action="store_true",
                    help="restore with ignore-unclaimed-state (like --allowNonRestoredState)")
    sp.set_defaults(fn=cmd_restore_test)

    sp = sub.add_parser("deploy", help="generate Kubernetes resources (FlinkDeployment, ConfigMap, Dockerfile)")
    sp.add_argument("path")
    sp.add_argument("--env")
    sp.add_argument("-o", "--out", default="deploy")
    sp.add_argument("--plan-dir", help="directory written by `cascade plan` (deploys EXECUTE PLAN with pinned UIDs)")
    sp.add_argument("--image", help="runtime image (overrides deploy.image)")
    sp.set_defaults(fn=cmd_deploy)

    sp = sub.add_parser("import-schema", help="generate source columns from a schema registry subject or .avsc")
    sp.add_argument("--registry")
    sp.add_argument("--subject")
    sp.add_argument("--version", default="latest")
    sp.add_argument("--avsc")
    sp.add_argument("--source", help="wrap the columns in a source block with this name")
    sp.set_defaults(fn=cmd_import_schema)

    sp = sub.add_parser("fetch-jars", help="download the pipeline's connector jars; prints CASCADE_EXTRA_JARS")
    sp.add_argument("path")
    sp.add_argument("--env")
    sp.set_defaults(fn=cmd_fetch_jars)

    sp = sub.add_parser("connectors", help="list available connectors and their properties")
    sp.set_defaults(fn=cmd_connectors)

    args = ap.parse_args(argv)
    return args.fn(args)
