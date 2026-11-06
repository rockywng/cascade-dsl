"""Parsing and normalization of Flink SQL data types used in DSL column declarations."""
from __future__ import annotations

import re
from dataclasses import dataclass

ALIASES = {
    "TEXT": "STRING",
    "LONG": "BIGINT",
    "INTEGER": "INT",
    "BOOL": "BOOLEAN",
    "BYTES": "BYTES",
    "FLOAT64": "DOUBLE",
    "FLOAT32": "FLOAT",
}

SIMPLE = {
    "STRING", "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE",
    "DATE", "BYTES",
}
PARAMETERIZED = {
    # name: (min params, max params)
    "CHAR": (0, 1), "VARCHAR": (0, 1), "BINARY": (0, 1), "VARBINARY": (0, 1),
    "DECIMAL": (0, 2), "TIME": (0, 1), "TIMESTAMP": (0, 1), "TIMESTAMP_LTZ": (0, 1),
}
NESTED = {"ARRAY", "MAP", "MULTISET", "ROW"}
NUMERIC = {"TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL"}


class TypeParseError(ValueError):
    pass


@dataclass(frozen=True)
class SqlType:
    """A normalized Flink type. `text` is the canonical rendering used in DDL."""

    name: str
    params: tuple = ()
    nullable: bool = True
    text: str = ""

    @property
    def is_temporal(self) -> bool:
        return self.name in ("TIMESTAMP", "TIMESTAMP_LTZ")

    @property
    def is_numeric(self) -> bool:
        return self.name in NUMERIC

    def with_nullable(self, nullable: bool) -> "SqlType":
        base = self.text[: -len(" NOT NULL")] if self.text.endswith(" NOT NULL") else self.text
        return SqlType(self.name, self.params, nullable, base if nullable else base + " NOT NULL")

    def same_base(self, other: "SqlType") -> bool:
        return self.with_nullable(True).text == other.with_nullable(True).text

    def __str__(self) -> str:
        return self.text


def parse_type(raw: str) -> SqlType:
    text = " ".join(str(raw).strip().split())
    if not text:
        raise TypeParseError("empty type")
    nullable = True
    m = re.match(r"^(.*?)\s+NOT\s+NULL$", text, re.IGNORECASE)
    if m:
        text, nullable = m.group(1), False
    t = _parse(text)
    return t if nullable else t.with_nullable(False)


def _parse(text: str) -> SqlType:
    m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*(.*)$", text)
    if not m:
        raise TypeParseError(f"cannot parse type '{text}'")
    name, rest = m.group(1).upper(), m.group(2).strip()
    name = ALIASES.get(name, name)

    if name in SIMPLE:
        if rest:
            raise TypeParseError(f"type {name} takes no parameters (got '{rest}')")
        return SqlType(name, (), True, name)

    if name in PARAMETERIZED:
        lo, hi = PARAMETERIZED[name]
        params: tuple = ()
        if rest:
            pm = re.match(r"^\(\s*([0-9\s,]*)\)$", rest)
            if not pm:
                raise TypeParseError(f"bad parameters for {name}: '{rest}'")
            params = tuple(int(p) for p in pm.group(1).split(",") if p.strip())
        if not lo <= len(params) <= hi:
            raise TypeParseError(f"{name} takes {lo}..{hi} parameters, got {len(params)}")
        if name == "DECIMAL" and params:
            p = params[0]
            s = params[1] if len(params) > 1 else 0
            if not (1 <= p <= 38 and 0 <= s <= p):
                raise TypeParseError(f"DECIMAL({p}, {s}) out of range (precision 1..38, 0 <= scale <= precision)")
            params = (p, s)
        if name in ("TIMESTAMP", "TIMESTAMP_LTZ", "TIME") and params and not 0 <= params[0] <= 9:
            raise TypeParseError(f"{name} precision must be 0..9")
        rendered = f"{name}({', '.join(map(str, params))})" if params else name
        return SqlType(name, params, True, rendered)

    if name in NESTED:
        inner = _strip_angle(rest, name)
        if name in ("ARRAY", "MULTISET"):
            e = parse_type(inner)
            return SqlType(name, (e,), True, f"{name}<{e.text}>")
        if name == "MAP":
            parts = _split_top(inner)
            if len(parts) != 2:
                raise TypeParseError("MAP takes exactly two type arguments")
            k, v = parse_type(parts[0]), parse_type(parts[1])
            return SqlType(name, (k, v), True, f"MAP<{k.text}, {v.text}>")
        fields = []
        for part in _split_top(inner):
            fm = re.match(r"^`?([A-Za-z_][A-Za-z0-9_]*)`?\s+(.+)$", part.strip())
            if not fm:
                raise TypeParseError(f"bad ROW field '{part.strip()}'")
            fields.append((fm.group(1), parse_type(fm.group(2))))
        rendered = "ROW<" + ", ".join(f"`{n}` {t.text}" for n, t in fields) + ">"
        return SqlType(name, tuple(fields), True, rendered)

    raise TypeParseError(f"unknown type '{name}'")


def _strip_angle(rest: str, name: str) -> str:
    if not (rest.startswith("<") and rest.endswith(">")):
        raise TypeParseError(f"{name} requires <...> type arguments")
    return rest[1:-1]


def _split_top(s: str) -> list[str]:
    parts, depth, cur = [], 0, []
    for ch in s:
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        parts.append("".join(cur))
    return parts


# --- result-type rules used by the static schema inference ----------------------------

BIGINT = parse_type("BIGINT")
BOOLEAN = parse_type("BOOLEAN")
STRING = parse_type("STRING")
DOUBLE = parse_type("DOUBLE")
TIMESTAMP3 = parse_type("TIMESTAMP(3)")


def sum_type(arg: SqlType | None) -> SqlType | None:
    if arg is None:
        return None
    if arg.name == "DECIMAL":
        scale = arg.params[1] if len(arg.params) > 1 else 0
        return parse_type(f"DECIMAL(38, {scale})")
    if arg.name in ("TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE"):
        return arg.with_nullable(True)
    return None
