"""Compiler entry point: load -> analyze/validate -> lower."""
from __future__ import annotations

from dataclasses import dataclass

from .analyze import Checked, check
from .diagnostics import CompileError, Diagnostics
from .loader import load_pipeline


def load_project(path: str, env: str | None = None):
    """A project is a directory with pipeline.yaml or pipeline.py, or one of those files."""
    import os
    if os.path.isdir(path):
        py, yml = os.path.join(path, "pipeline.py"), os.path.join(path, "pipeline.yaml")
        path = yml if os.path.exists(yml) or not os.path.exists(py) else py
    if path.endswith(".py"):
        from .api import load_python
        return load_python(path, env)
    return load_pipeline(path, env)
from .lower import Lowered, lower


@dataclass
class Result:
    diagnostics: Diagnostics
    checked: Checked | None = None
    lowered: Lowered | None = None

    @property
    def ok(self) -> bool:
        return not self.diagnostics.has_errors and self.lowered is not None


def compile_pipeline(path: str, env: str | None = None, emit: bool = True, use_lock: bool = True) -> Result:
    try:
        pipeline, diags = load_project(path, env)
    except CompileError as e:
        return Result(e.diagnostics)
    if use_lock:
        from .schemas import read_lock
        pipeline.schema_lock = read_lock(pipeline.root_dir)
    checked = check(pipeline, diags)
    if diags.has_errors or not emit:
        return Result(diags, checked)
    lowered = lower(checked)
    from .udfs import write_python_dir
    write_python_dir(checked.pipeline)
    return Result(diags, checked, lowered)
