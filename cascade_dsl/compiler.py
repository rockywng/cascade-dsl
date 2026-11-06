"""Compiler entry point: load -> analyze/validate -> lower."""
from __future__ import annotations

from dataclasses import dataclass

from .analyze import Checked, check
from .diagnostics import CompileError, Diagnostics
from .loader import load_pipeline
from .lower import Lowered, lower


@dataclass
class Result:
    diagnostics: Diagnostics
    checked: Checked | None = None
    lowered: Lowered | None = None

    @property
    def ok(self) -> bool:
        return not self.diagnostics.has_errors and self.lowered is not None


def compile_pipeline(path: str, env: str | None = None, emit: bool = True) -> Result:
    try:
        pipeline, diags = load_pipeline(path, env)
    except CompileError as e:
        return Result(e.diagnostics)
    checked = check(pipeline, diags)
    if diags.has_errors or not emit:
        return Result(diags, checked)
    return Result(diags, checked, lower(checked))
