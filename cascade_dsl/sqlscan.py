"""Lightweight static analysis of Flink SQL model queries.

This is deliberately *not* a full SQL compiler: it tokenizes the query and
recognizes the structural patterns that matter to the platform (references,
stateful operators, changelog mode, time attributes, output columns). It is
fast and JVM-free so it runs in editors and CI. The authoritative check is
`cascade verify`, which plans the generated SQL with the real Flink planner and
cross-checks these static facts.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from .ir import OutputColumn, RelationInfo, StatefulOp
from .types import (BIGINT, STRING, TIMESTAMP3, SqlType, TypeParseError, parse_type,
                    sum_type)

KEYWORDS = {
    "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT", "JOIN", "INNER",
    "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "ON", "AS", "AND", "OR", "NOT", "IN", "IS",
    "NULL", "TRUE", "FALSE", "CASE", "WHEN", "THEN", "ELSE", "END", "UNION", "ALL",
    "DISTINCT", "WITH", "OVER", "PARTITION", "ROWS", "RANGE", "BETWEEN", "PRECEDING",
    "FOLLOWING", "CURRENT", "ROW", "UNBOUNDED", "INTERVAL", "TABLE", "LATERAL", "UNNEST",
    "FOR", "SYSTEM_TIME", "OF", "DESCRIPTOR", "CAST", "TRY_CAST", "LIKE", "EXISTS", "ASC",
    "DESC", "WINDOW", "MATCH_RECOGNIZE", "INTERSECT", "EXCEPT", "MINUS", "VALUES", "USING",
    "SECOND", "MINUTE", "HOUR", "DAY", "MONTH", "YEAR", "MILLISECOND", "TO", "ESCAPE",
    "SIMILAR", "NULLS", "FIRST", "LAST", "FETCH", "OFFSET", "QUALIFY",
}
AGG_FUNCS = {"COUNT", "SUM", "MIN", "MAX", "AVG", "LISTAGG", "COLLECT", "STDDEV_POP",
             "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "FIRST_VALUE", "LAST_VALUE", "ARRAY_AGG",
             "JSON_ARRAYAGG", "JSON_OBJECTAGG"}
STRING_FUNCS = {"UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM", "CONCAT", "CONCAT_WS", "SUBSTRING",
                "SUBSTR", "REPLACE", "REGEXP_REPLACE", "REGEXP_EXTRACT", "INITCAP", "LPAD",
                "RPAD", "MD5", "SHA256", "JSON_VALUE", "DATE_FORMAT", "UUID"}
WINDOW_TVFS = {"TUMBLE", "HOP", "CUMULATE", "SESSION"}
WINDOW_COLS = {"window_start", "window_end", "window_time"}
CLAUSE_END = {"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT",
              "WINDOW", "QUALIFY"}
JOIN_WORDS = {"JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "OUTER"}


@dataclass
class Tok:
    kind: str  # ident, qident, string, number, op, hint
    text: str
    pos: int
    depth: int = 0

    @property
    def up(self) -> str:
        return self.text.upper() if self.kind == "ident" else self.text

    @property
    def name(self) -> str:
        """Identifier value with backticks removed."""
        return self.text[1:-1] if self.kind == "qident" else self.text

    def is_kw(self, *words) -> bool:
        return self.kind == "ident" and self.text.upper() in words

    @property
    def is_name(self) -> bool:
        return self.kind == "qident" or (self.kind == "ident" and self.text.upper() not in KEYWORDS)


class SqlSyntaxError(ValueError):
    def __init__(self, msg, pos):
        super().__init__(msg)
        self.pos = pos


_TOKEN_RE = re.compile(r"""
    (?P<ws>\s+)
  | (?P<hint>/\*\+.*?\*/)
  | (?P<bcomment>/\*.*?\*/)
  | (?P<lcomment>--[^\n]*)
  | (?P<string>'(?:[^']|'')*')
  | (?P<qident>`(?:[^`]|``)*`)
  | (?P<number>\d+(?:\.\d+)?)
  | (?P<ident>[A-Za-z_][A-Za-z0-9_$]*)
  | (?P<op><>|!=|>=|<=|=>|\|\||[(),.;*=<>+\-/%\[\]:])
""", re.VERBOSE | re.DOTALL)


def tokenize(sql: str) -> list[Tok]:
    toks, pos, depth = [], 0, 0
    while pos < len(sql):
        m = _TOKEN_RE.match(sql, pos)
        if not m:
            raise SqlSyntaxError(f"unexpected character {sql[pos]!r}", pos)
        kind = m.lastgroup
        text = m.group(kind)
        if kind not in ("ws", "bcomment", "lcomment"):
            if text == ")":
                depth -= 1
                if depth < 0:
                    raise SqlSyntaxError("unbalanced ')'", pos)
            toks.append(Tok(kind, text, pos, depth))
            if text == "(":
                depth += 1
        pos = m.end()
    if depth != 0:
        raise SqlSyntaxError("unbalanced '(' (missing ')')", len(sql))
    return toks


def line_of(sql: str, pos: int) -> int:
    """0-based line offset of `pos` inside `sql`."""
    return sql.count("\n", 0, pos)


def _match_paren(toks: list[Tok], i: int) -> int:
    """Index of the ')' closing the '(' at i."""
    d = toks[i].depth
    for j in range(i + 1, len(toks)):
        if toks[j].text == ")" and toks[j].depth == d:
            return j
    return len(toks) - 1


@dataclass
class Ref:
    name: str
    alias: str | None
    pos: int
    temporal: bool = False  # versioned side of FOR SYSTEM_TIME AS OF
    in_window_tvf: bool = False


@dataclass
class Analysis:
    refs: list[Ref] = field(default_factory=list)
    cte_names: set[str] = field(default_factory=set)
    windows: list[tuple[str, str, str, int]] = field(default_factory=list)  # fn, table, col, pos
    ops: list[StatefulOp] = field(default_factory=list)
    updating: bool = False
    select_items: list[list[Tok]] = field(default_factory=list)
    has_ttl_hint: bool = False
    ctes: list[tuple[str, int, int]] = field(default_factory=list)  # name, body start, body end (chars)
    subqueries: list[tuple[str, int, int]] = field(default_factory=list)  # alias, start, end of FROM (subquery)
    scope_refs: list[Ref] = field(default_factory=list)  # refs incl. CTE names, for column scoping
    group_keys: list[str] | None = None  # plain-column keys of the outermost non-windowed GROUP BY
    problems: list[tuple[str, str, int, str | None]] = field(default_factory=list)  # code,msg,pos,hint


def analyze_sql(sql: str) -> tuple[list[Tok], Analysis]:
    toks = tokenize(sql)
    a = Analysis()
    if not toks:
        a.problems.append(("CD301", "model SQL is empty", 0, None))
        return toks, a

    # Single SELECT statement only
    if not toks[0].is_kw("SELECT", "WITH"):
        a.problems.append(("CD301", f"model SQL must be a single SELECT query (starts with '{toks[0].text}')",
                           toks[0].pos, "DDL/DML is generated by the compiler; models are pure queries"))
    for i, t in enumerate(toks):
        if t.text == ";" and i != len(toks) - 1:
            a.problems.append(("CD301", "model SQL must contain exactly one statement", t.pos, None))
            break
    if toks[-1].text == ";":
        toks = toks[:-1]

    a.has_ttl_hint = any(t.kind == "hint" and "STATE_TTL" in t.text.upper() for t in toks)
    _find_ctes(toks, a)
    _find_refs(toks, a)
    _find_windows(toks, a)
    _find_stateful(toks, a)
    a.select_items = _main_select_items(toks)
    return toks, a


def _find_ctes(toks, a: Analysis):
    if not toks or not toks[0].is_kw("WITH"):
        return
    i = 1
    while i < len(toks):
        if toks[i].is_name and i + 2 < len(toks) and toks[i + 1].is_kw("AS") and toks[i + 2].text == "(":
            a.cte_names.add(toks[i].name)
            close = _match_paren(toks, i + 2)
            a.ctes.append((toks[i].name, toks[i + 2].pos + 1, toks[close].pos))
            i = close + 1
            if i < len(toks) and toks[i].text == ",":
                i += 1
                continue
        break


def _qualified_name(toks, i) -> tuple[str, int]:
    """Read a possibly dotted identifier starting at i; returns (name, next index)."""
    parts = [toks[i].name]
    j = i + 1
    while j + 1 < len(toks) and toks[j].text == "." and toks[j + 1].kind in ("ident", "qident"):
        parts.append(toks[j + 1].name)
        j += 2
    return ".".join(parts), j


def _read_alias(toks, j) -> tuple[str | None, int]:
    if j < len(toks) and toks[j].is_kw("AS") and j + 1 < len(toks) and toks[j + 1].kind in ("ident", "qident"):
        return toks[j + 1].name, j + 2
    if j < len(toks) and toks[j].is_name:
        return toks[j].name, j + 1
    return None, j


def _find_refs(toks, a: Analysis):
    n = len(toks)
    i = 0
    while i < n:
        t = toks[i]
        # TABLE <name> inside table-valued functions (window TVFs etc.)
        if t.is_kw("TABLE") and i + 1 < n and toks[i + 1].kind in ("ident", "qident") \
                and not (i > 0 and toks[i - 1].is_kw("LATERAL")):
            name, j = _qualified_name(toks, i + 1)
            if name.upper() not in KEYWORDS:
                a.refs.append(Ref(name, None, toks[i + 1].pos, in_window_tvf=True))
            i = j
            continue
        if t.is_kw("FROM", "JOIN"):
            j = i + 1
            depth = t.depth
            if depth == 0 and j + 1 < n and toks[j].text == "(" and toks[j + 1].is_kw("SELECT", "WITH"):
                close = _match_paren(toks, j)
                alias, _ = _read_alias(toks, close + 1)
                a.subqueries.append((alias or "", toks[j].pos + 1, toks[close].pos))
            while j < n:
                if toks[j].is_kw("LATERAL", "UNNEST") or toks[j].text == "(" or toks[j].is_kw("TABLE"):
                    break  # subquery / TVF / lateral: handled elsewhere
                if toks[j].kind not in ("ident", "qident") or not (toks[j].is_name or toks[j].kind == "qident"):
                    break
                name, j = _qualified_name(toks, j)
                ref = Ref(name, None, toks[j - 1].pos)
                # FOR SYSTEM_TIME AS OF <expr>
                if j + 3 < n and toks[j].is_kw("FOR") and toks[j + 1].is_kw("SYSTEM_TIME"):
                    ref.temporal = True
                    j += 4  # FOR SYSTEM_TIME AS OF
                    while j < n and not (toks[j].depth == depth and (
                            toks[j].is_kw("AS", "ON", "WHERE", "USING") or toks[j].up in JOIN_WORDS
                            or toks[j].up in CLAUSE_END or toks[j].text in (",", ")"))):
                        j += 1
                ref.alias, j = _read_alias(toks, j)
                a.refs.append(ref)
                # comma join: FROM a, b
                if t.is_kw("FROM") and j < n and toks[j].text == "," and toks[j].depth == depth:
                    j += 1
                    continue
                break
            i = max(j, i + 1)
            continue
        i += 1
    # CTE names are not external references, but they are in scope for column resolution
    a.scope_refs = list(a.refs)
    a.refs = [r for r in a.refs if r.name not in a.cte_names]


def _find_windows(toks, a: Analysis):
    for i, t in enumerate(toks):
        if t.is_kw(*WINDOW_TVFS) and i + 1 < len(toks) and toks[i + 1].text == "(":
            end = _match_paren(toks, i + 1)
            table = col = None
            for j in range(i + 2, end):
                if toks[j].is_kw("TABLE") and toks[j + 1].kind in ("ident", "qident"):
                    table = _qualified_name(toks, j + 1)[0]
                if toks[j].is_kw("DESCRIPTOR") and toks[j + 1].text == "(":
                    col = toks[j + 2].name
            if table:
                a.windows.append((t.up, table, col or "", t.pos))


def _clause_tokens(toks, start, depth, stop_words) -> list[Tok]:
    out = []
    for t in toks[start:]:
        if t.depth < depth or (t.depth == depth and (t.up in stop_words or t.text == ")")):
            break
        out.append(t)
    return out


def _find_stateful(toks, a: Analysis):
    n = len(toks)
    has_group_by = False
    for i, t in enumerate(toks):
        # GROUP BY
        if t.is_kw("GROUP") and i + 1 < n and toks[i + 1].is_kw("BY"):
            has_group_by = True
            keys = _clause_tokens(toks, i + 2, t.depth, {"HAVING", "ORDER", "LIMIT", "UNION", "WINDOW"} | {"QUALIFY"})
            names = {k.name.lower() for k in keys if k.kind in ("ident", "qident")}
            if names & {"window_start", "window_end"} and a.windows:
                a.ops.append(StatefulOp("window_agg", True, "windowed aggregation, state freed by watermark"))
            else:
                a.ops.append(StatefulOp("group_agg", False, "non-windowed GROUP BY keeps one entry per key forever"))
                a.updating = True
                if t.depth == 0:
                    parts = [[]]
                    for k in keys:
                        if k.text == "," and k.depth == t.depth:
                            parts.append([])
                        else:
                            parts[-1].append(k)
                    plain = [p[-1].name for p in parts if len(p) == 1 or (len(p) == 3 and p[1].text == ".")]
                    a.group_keys = plain if len(plain) == len(parts) else None

        # JOINs
        if t.is_kw("JOIN"):
            if i + 1 < n and toks[i + 1].is_kw("LATERAL", "UNNEST"):
                continue
            prev = [x.up for x in toks[max(0, i - 2):i]]
            outer = any(w in prev for w in ("LEFT", "RIGHT", "FULL"))
            ref = next((r for r in a.refs if r.pos > t.pos), None)
            if ref is not None and ref.temporal:
                a.ops.append(StatefulOp("temporal_join", True, "versioned table join, state bounded by watermark"))
                continue
            on_toks = []
            for j in range(i + 1, n):
                if toks[j].is_kw("ON") and toks[j].depth == t.depth:
                    on_toks = _clause_tokens(toks, j + 1, t.depth, CLAUSE_END | JOIN_WORDS)
                    break
            on_names = {x.name.lower() for x in on_toks if x.kind in ("ident", "qident")}
            if {"window_start", "window_end"} <= on_names:
                a.ops.append(StatefulOp("window_join", True, "window join, state freed by watermark"))
            elif any(x.is_kw("INTERVAL") for x in on_toks):
                a.ops.append(StatefulOp("interval_join", True, "interval join, state bounded by time range"))
            elif toks[i - 1].is_kw("CROSS") if i > 0 else False:
                a.ops.append(StatefulOp("regular_join", False, "cross join keeps both inputs forever"))
                a.updating = True
            else:
                kind = "outer" if outer else "inner"
                a.ops.append(StatefulOp("regular_join", False, f"regular {kind} join keeps both inputs forever"))
                if outer:
                    a.updating = True

        # ROW_NUMBER() OVER (...) => dedup / top-n
        if t.is_kw("ROW_NUMBER") and i + 3 < n and toks[i + 3].is_kw("OVER"):
            over_end = _match_paren(toks, i + 4) if toks[i + 4].text == "(" else i + 4
            over = toks[i + 5:over_end]
            order_idx = next((k for k, x in enumerate(over) if x.is_kw("ORDER")), None)
            order_toks = over[order_idx + 2:] if order_idx is not None else []
            desc = any(x.is_kw("DESC") for x in order_toks)
            key = " ".join(x.text for x in order_toks if not x.is_kw("ASC", "DESC"))
            a.ops.append(StatefulOp("dedup", False, f"ROW_NUMBER() ordered by {key or '?'} {'DESC' if desc else 'ASC'}"))
            a.updating = a.updating or desc  # keep-last / top-n emit updates; keep-first-by-time is append
            a.ops[-1].detail += ("; keep-last emits updates" if desc else "")

        # OVER aggregations (not ROW_NUMBER)
        if t.is_kw("OVER") and i > 0 and toks[i - 1].text == ")":
            open_i = next(k for k in range(i - 1, -1, -1) if toks[k].text == "(" and toks[k].depth == toks[i - 1].depth)
            fn = toks[open_i - 1].up if open_i > 0 else ""
            if fn != "ROW_NUMBER":
                spec_end = _match_paren(toks, i + 1) if i + 1 < n and toks[i + 1].text == "(" else i
                spec = toks[i + 1:spec_end]
                unbounded = any(x.is_kw("UNBOUNDED") for x in spec) or not any(x.is_kw("ROWS", "RANGE") for x in spec)
                a.ops.append(StatefulOp("over_agg", not unbounded,
                                        "OVER aggregation" + (" with UNBOUNDED PRECEDING" if unbounded else "")))

        if t.is_kw("DISTINCT") and i > 0 and toks[i - 1].is_kw("SELECT") and not has_group_by:
            a.ops.append(StatefulOp("distinct", False, "SELECT DISTINCT keeps every distinct row forever"))

        if t.is_kw("MATCH_RECOGNIZE"):
            a.ops.append(StatefulOp("match_recognize", True, "pattern matching, bounded by WITHIN/watermark"))

    # aggregate calls without GROUP BY and without OVER => global aggregate
    if not has_group_by:
        for i, t in enumerate(toks):
            if t.kind == "ident" and t.up in AGG_FUNCS and i + 1 < n and toks[i + 1].text == "(":
                close = _match_paren(toks, i + 1)
                if not (close + 1 < n and toks[close + 1].is_kw("OVER")):
                    a.ops.append(StatefulOp("global_agg", True, "aggregate without GROUP BY (single key)"))
                    a.updating = True
                    break


def _main_select_items(toks) -> list[list[Tok]]:
    """Split the select list of the outermost query into items."""
    start = next((i for i, t in enumerate(toks) if t.depth == 0 and t.is_kw("SELECT")), None)
    if start is None:
        return []
    i = start + 1
    while i < len(toks) and (toks[i].kind == "hint" or toks[i].is_kw("DISTINCT", "ALL")):
        i += 1
    items, cur = [], []
    while i < len(toks):
        t = toks[i]
        if t.depth == 0 and t.is_kw("FROM"):
            break
        if t.depth == 0 and t.text == ",":
            items.append(cur)
            cur = []
        else:
            cur.append(t)
        i += 1
    if cur:
        items.append(cur)
    return items


# --- relation inference ------------------------------------------------------------------

@dataclass
class Scope:
    """Columns visible to a query: input relation infos addressable by name or alias."""

    by_alias: dict[str, RelationInfo]
    rowtime_aliases: set[str]  # aliases whose time attributes survive the query's joins
    window_rel: RelationInfo | None = None
    functions: dict = field(default_factory=dict)  # user function name (upper) -> return SqlType

    def lookup(self, qualifier: str | None, col: str) -> tuple[OutputColumn | None, bool, bool]:
        """Return (column, is_rowtime, ambiguous)."""
        if col.lower() in WINDOW_COLS and self.window_rel is not None and qualifier is None:
            return self.window_rel.column(col), col.lower() == "window_time", False
        cands = []
        for alias, rel in self.by_alias.items():
            if qualifier is not None and alias != qualifier:
                continue
            c = rel.column(col)
            if c is not None:
                rowtime = alias in self.rowtime_aliases and c.name in rel.rowtime
                cands.append((c, rowtime))
        if not cands:
            return None, False, False
        return cands[0][0], cands[0][1], len(cands) > 1 and qualifier is None


def infer_output(items: list[list[Tok]], scope: Scope, grouped: bool) -> tuple[list[OutputColumn] | None, set[str]]:
    cols: list[OutputColumn] = []
    rowtime: set[str] = set()
    for item in items:
        if not item:
            return None, set()
        # star expansion
        if item[-1].text == "*" and (len(item) == 1 or (len(item) == 3 and item[1].text == ".")):
            qual = item[0].name if len(item) == 3 else None
            rels = [(al, r) for al, r in scope.by_alias.items() if qual is None or al == qual]
            if scope.window_rel is not None and qual is None:
                rels = [("", scope.window_rel)]
            if not rels:
                return None, set()  # star over a CTE/subquery: opaque to static inference
            for al, r in rels:
                if r.columns is None:
                    return None, set()
                cols.extend(r.columns)
                if al in scope.rowtime_aliases or al == "":
                    rowtime |= set(r.rowtime)
            continue

        alias = None
        expr = item
        if len(item) >= 3 and item[-2].is_kw("AS") and item[-1].kind in ("ident", "qident"):
            alias, expr = item[-1].name, item[:-2]
        elif len(item) >= 2 and item[-1].is_name and item[-2].text not in (".",) and not item[-2].kind == "op" \
                and not item[-2].is_kw(*KEYWORDS - {"END"}):
            alias, expr = item[-1].name, item[:-1]
        elif len(item) >= 2 and item[-1].is_name and item[-2].text == ")":
            alias, expr = item[-1].name, item[:-1]

        typ, is_rt, name = _infer_expr(expr, scope)
        out_name = alias or name
        if out_name is None:
            return None, set()  # unnamed expression, Flink would call it EXPR$n
        cols.append(OutputColumn(out_name, typ, " ".join(t.text for t in expr)))
        if is_rt and not grouped:
            rowtime.add(out_name)
        elif expr and len(expr) == 1 and expr[0].name.lower() == "window_time":
            rowtime.add(out_name)
    return (cols or None), rowtime


def _infer_expr(expr: list[Tok], scope: Scope) -> tuple[SqlType | None, bool, str | None]:
    """Returns (type or None, is_rowtime, implicit name or None)."""
    if len(expr) == 1 and expr[0].kind in ("ident", "qident") and expr[0].is_name or \
            (len(expr) == 1 and expr[0].kind == "ident" and expr[0].name.lower() in WINDOW_COLS):
        c, rt, _ = scope.lookup(None, expr[0].name)
        return (c.type if c else None), rt, expr[0].name
    if len(expr) == 3 and expr[1].text == "." and expr[0].kind in ("ident", "qident") and expr[2].kind in ("ident", "qident"):
        c, rt, _ = scope.lookup(expr[0].name, expr[2].name)
        return (c.type if c else None), rt, expr[2].name
    if len(expr) == 1 and expr[0].kind == "string":
        return STRING, False, None
    if len(expr) == 1 and expr[0].kind == "number":
        return (parse_type("INT") if "." not in expr[0].text else None), False, None

    over = next((k for k, t in enumerate(expr) if t.is_kw("OVER") and t.depth == expr[0].depth), None)
    if over is not None:
        expr = expr[:over]  # type of `f(..) OVER (..)` is the type of f(..)
        if expr and expr[0].is_kw("ROW_NUMBER", "RANK", "DENSE_RANK"):
            return BIGINT.with_nullable(False), False, None
    head = expr[0] if expr else None
    if head is not None and head.kind == "ident" and len(expr) >= 3 and expr[1].text == "(" \
            and _match_paren(expr, 1) == len(expr) - 1:
        fn = head.up
        inner = expr[2:-1]
        if fn in ("CAST", "TRY_CAST"):
            as_idx = max((k for k, t in enumerate(inner) if t.is_kw("AS") and t.depth == expr[1].depth + 1), default=None)
            if as_idx is not None:
                try:
                    return parse_type(" ".join(t.text for t in inner[as_idx + 1:]).replace(" ( ", "(").replace(" )", ")")), False, None
                except TypeParseError:
                    return None, False, None
        if fn == "COUNT":
            return BIGINT.with_nullable(False), False, None
        arg_t = _infer_expr(inner, scope)[0] if inner else None
        if fn == "SUM":
            return sum_type(arg_t), False, None
        if fn in ("MIN", "MAX", "FIRST_VALUE", "LAST_VALUE"):
            return (arg_t.with_nullable(True) if arg_t else None), False, None
        if fn == "AVG":
            return (arg_t if arg_t and arg_t.name in ("INT", "BIGINT", "DOUBLE", "FLOAT") else None), False, None
        if fn in STRING_FUNCS:
            return STRING, False, None
        if fn == "COALESCE":
            first = _split_args(inner)
            for arg in first:
                t = _infer_expr(arg, scope)[0]
                if t is not None:
                    return t, False, None
            return None, False, None
        if fn in scope.functions:
            return scope.functions[fn], False, None
        if fn in ("CURRENT_TIMESTAMP", "NOW", "LOCALTIMESTAMP"):
            return parse_type("TIMESTAMP_LTZ(3)"), False, None
        if fn in ("TO_TIMESTAMP",):
            return TIMESTAMP3, False, None
    if len(expr) == 1 and expr[0].is_kw("CURRENT_TIMESTAMP"):
        return parse_type("TIMESTAMP_LTZ(3)"), False, None
    return None, False, None


def _split_args(inner: list[Tok]) -> list[list[Tok]]:
    if not inner:
        return []
    d = inner[0].depth
    args, cur = [], []
    for t in inner:
        if t.text == "," and t.depth == d:
            args.append(cur)
            cur = []
        else:
            cur.append(t)
    args.append(cur)
    return args


def window_relation(base: RelationInfo) -> RelationInfo:
    cols = list(base.columns or []) + [
        OutputColumn("window_start", TIMESTAMP3.with_nullable(False)),
        OutputColumn("window_end", TIMESTAMP3.with_nullable(False)),
        OutputColumn("window_time", TIMESTAMP3.with_nullable(False)),
    ]
    return RelationInfo(columns=cols if base.columns is not None else None,
                        rowtime=set(base.rowtime) | {"window_time"}, changelog=base.changelog)


def referenced_columns(expr_sql: str) -> list[tuple[str, int]]:
    """Identifiers in a boolean check expression that look like column references."""
    toks = tokenize(expr_sql)
    out = []
    for i, t in enumerate(toks):
        if not t.is_name:
            continue
        if i + 1 < len(toks) and toks[i + 1].text in ("(", "."):
            continue  # function call or qualifier
        if i > 0 and toks[i - 1].text == ".":
            continue
        out.append((t.name, t.pos))
    return out


def _within(pos: int, spans) -> bool:
    return any(s <= pos < e for _, s, e in spans)


def infer_relation(sql: str, rels: dict[str, RelationInfo],
                   functions: dict | None = None) -> tuple[list[OutputColumn] | None, set[str]]:
    """Infer (output columns, time attributes) of a query, recursing into CTEs and FROM subqueries.
    `functions` maps user function names (upper case) to their return types."""
    try:
        _toks, a = analyze_sql(sql)
    except SqlSyntaxError:
        return None, set()
    local = dict(rels)
    for name, s, e in a.ctes:
        cols, rt = infer_relation(sql[s:e], local, functions)
        local[name] = RelationInfo(columns=cols, rowtime=rt)
    spans = a.ctes + a.subqueries
    outer_refs = [r for r in a.scope_refs if not _within(r.pos, spans)]
    by_alias: dict[str, RelationInfo] = {}
    for r in outer_refs:
        if r.name in local and not r.in_window_tvf:
            by_alias[r.alias or r.name] = local[r.name]
    for alias, s, e in a.subqueries:
        cols, rt = infer_relation(sql[s:e], local, functions)
        by_alias[alias] = RelationInfo(columns=cols, rowtime=rt)
    kinds = {op.kind for op in a.ops}
    if "regular_join" in kinds:
        rt_aliases: set[str] = set()  # regular joins materialize time attributes
    elif "temporal_join" in kinds:
        rt_aliases = {r.alias or r.name for r in outer_refs if not r.temporal}
    else:
        rt_aliases = set(by_alias)
    window_rel = None
    for _fn, table, _col, pos in a.windows:
        if table in local and not _within(pos, spans):
            window_rel = window_relation(local[table])
            by_alias.setdefault(table, local[table])
    grouped = bool(kinds & {"group_agg", "global_agg", "window_agg"})
    return infer_output(a.select_items, Scope(by_alias, rt_aliases, window_rel, dict(functions or {})), grouped)
