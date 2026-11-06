"""Compiler diagnostics: every problem carries a stable code, a location and a hint."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True)
class Location:
    file: str
    line: int | None = None

    def __str__(self) -> str:
        return f"{self.file}:{self.line}" if self.line else self.file


@dataclass
class Diagnostic:
    code: str
    severity: Severity
    message: str
    location: Location | None = None
    hint: str | None = None
    node: str | None = None

    def format(self) -> str:
        loc = f"{self.location}: " if self.location else ""
        out = f"{loc}{self.severity.value}[{self.code}]: {self.message}"
        if self.hint:
            out += f"\n    hint: {self.hint}"
        return out

    def to_json(self) -> dict:
        return {
            "code": self.code,
            "severity": self.severity.value,
            "message": self.message,
            "location": str(self.location) if self.location else None,
            "hint": self.hint,
            "node": self.node,
        }


@dataclass
class Diagnostics:
    items: list[Diagnostic] = field(default_factory=list)

    def error(self, code, message, location=None, hint=None, node=None):
        self.items.append(Diagnostic(code, Severity.ERROR, message, location, hint, node))

    def warning(self, code, message, location=None, hint=None, node=None):
        self.items.append(Diagnostic(code, Severity.WARNING, message, location, hint, node))

    def info(self, code, message, location=None, hint=None, node=None):
        self.items.append(Diagnostic(code, Severity.INFO, message, location, hint, node))

    @property
    def errors(self) -> list[Diagnostic]:
        return [d for d in self.items if d.severity == Severity.ERROR]

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)

    def codes(self) -> list[str]:
        return [d.code for d in self.items]


class CompileError(Exception):
    """Raised when compilation cannot continue; carries the diagnostics so far."""

    def __init__(self, diagnostics: Diagnostics):
        self.diagnostics = diagnostics
        super().__init__("\n".join(d.format() for d in diagnostics.errors))
