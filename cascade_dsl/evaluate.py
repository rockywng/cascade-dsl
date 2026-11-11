"""Run a Python pipeline and print the document it defines, for a compiler that must not run code.

    python -m cascade_dsl.evaluate <project dir>          # pipeline.py -> its document

The control plane never executes user Python: a short-lived pod in the tenant's own namespace runs
this (the platform's builder image), and the control plane compiles the printed document exactly as
it compiles pipeline.yaml. The output is one line on stdout:

    CASCADE-RESULT {"ok": true, "document": {...}}
    CASCADE-RESULT {"ok": false, "errors": ["pipeline.py:7: error[CD000]: ..."]}

The document is plain data (the same mapping pipeline.yaml would hold). Python UDF modules the
pipeline names are files of the project and are only copied by the compiler, never imported here.
"""
from __future__ import annotations

import json
import os
import sys

MARKER = "CASCADE-RESULT "


def _plain(v):
    """LDict/LList (with source positions) -> dict/list."""
    if isinstance(v, dict):
        return {str(k): _plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_plain(x) for x in v]
    return v


def evaluate(project: str) -> dict:
    from .api import Pipeline
    from .diagnostics import CompileError
    import runpy

    path = os.path.join(project, "pipeline.py") if os.path.isdir(project) else project
    root = os.path.dirname(os.path.abspath(path))
    sys.path.insert(0, root)
    try:
        ns = runpy.run_path(path, run_name="__cascade__")
    except CompileError as e:
        return {"ok": False, "errors": [d.format() for d in e.diagnostics.errors]}
    except BaseException as e:  # noqa: BLE001 - the author's code: report, don't crash
        tb, line = e.__traceback__, None
        while tb is not None:
            if os.path.abspath(tb.tb_frame.f_code.co_filename) == os.path.abspath(path):
                line = tb.tb_lineno
            tb = tb.tb_next
        return {"ok": False, "errors": [f"pipeline.py{f':{line}' if line else ''}: error[CD000]: "
                                        f"pipeline.py raised {type(e).__name__}: {e}"]}
    finally:
        sys.path.remove(root)
    found = ns.get("pipeline")
    if not isinstance(found, Pipeline):
        candidates = [v for v in ns.values() if isinstance(v, Pipeline)]
        if len(candidates) != 1:
            return {"ok": False, "errors": ["pipeline.py: error[CD000]: pipeline.py must define exactly one "
                                            "Pipeline (or name it `pipeline`)"]}
        found = candidates[0]
    try:
        doc = found.to_document("pipeline.py")
    except CompileError as e:
        return {"ok": False, "errors": [d.format() for d in e.diagnostics.errors]}
    return {"ok": True, "document": _plain(doc)}


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        print("usage: python -m cascade_dsl.evaluate <project dir | pipeline.py>", file=sys.stderr)
        return 2
    result = evaluate(argv[0])
    sys.stdout.write(MARKER + json.dumps(result, default=str) + "\n")
    sys.stdout.flush()
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
