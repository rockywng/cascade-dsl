"""DataFrame-style query builder that generates Flink SQL.

Only streaming-safe operations exist: no ORDER BY / LIMIT on unbounded input, windows require a
time attribute, joins say what kind of join they are. The output is ordinary SQL, so everything
the compiler checks for hand-written models applies unchanged.

    orders.where("amount > 0") \
          .tumble("order_ts", "1m") \
          .group_by("currency") \
          .agg(revenue=sum_("amount"), orders=count())
"""
from __future__ import annotations

import textwrap
from dataclasses import dataclass, field, replace

from .loader import parse_duration_ms


class BuilderError(ValueError):
    pass


# --- expression helpers -------------------------------------------------------------------

def sum_(expr: str) -> str: return f"SUM({expr})"
def min_(expr: str) -> str: return f"MIN({expr})"
def max_(expr: str) -> str: return f"MAX({expr})"
def avg(expr: str) -> str: return f"AVG({expr})"
def count(expr: str = "*") -> str: return f"COUNT({expr})"
def count_distinct(expr: str) -> str: return f"COUNT(DISTINCT {expr})"


def lit(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def interval(duration) -> str:
    ms = parse_duration_ms(duration)
    for unit, size in (("DAY", 86_400_000), ("HOUR", 3_600_000), ("MINUTE", 60_000), ("SECOND", 1000)):
        if ms % size == 0:
            return f"INTERVAL '{ms // size}' {unit}"
    return f"INTERVAL '{ms / 1000:.3f}' SECOND"


def _q(name: str) -> str:
    """Quote a generated alias: many natural names (views, value, user, ...) are reserved words."""
    return "`" + name.replace("`", "``") + "`"


@dataclass(frozen=True)
class Query:
    from_: str
    base: str | None = None  # relation name when FROM is a bare table
    columns: tuple | None = None  # known output columns (sources only), for dedup
    sel: tuple | None = None  # SELECT items (None = *)
    conds: tuple = ()  # WHERE conjuncts
    keys: tuple | None = None  # GROUP BY keys
    windowed: bool = False
    joined: bool = False
    rank_col: str | None = None
    depth: int = 0  # subquery nesting, used for deterministic aliases
    _alias: str | None = field(default=None, compare=False)

    # -- structure helpers
    @property
    def _bare(self) -> bool:
        return self.base is not None and not (self.sel or self.conds or self.keys or self.joined
                                              or self.windowed)

    def _wrap(self) -> "Query":
        alias = f"q{self.depth + 1}"
        return Query(from_=f"(\n{textwrap.indent(self.to_sql(), '  ')}\n) AS {alias}", _alias=alias,
                     columns=self._output_columns(), depth=self.depth + 1)

    def _output_columns(self):
        if self.sel is not None and all(" " not in s and "(" not in s and s != "*" for s in self.sel):
            return tuple(self.sel)
        if self.sel is None and self.rank_col is None and not self.joined:
            return self.columns
        return None

    # -- operations
    def where(self, *conditions: str) -> "Query":
        q = self if not (self.sel or self.keys) else self._wrap()
        return replace(q, conds=q.conds + tuple(f"({c})" for c in conditions), base=q.base)

    filter = where

    def select(self, *columns: str, **named: str) -> "Query":
        q = self._wrap() if self.sel is not None or self.keys is not None else self
        items = tuple(columns) + tuple(f"{expr} AS {_q(alias)}" for alias, expr in named.items())
        if not items:
            raise BuilderError("select() needs at least one column")
        return replace(q, sel=items, rank_col=None)

    def with_columns(self, **named: str) -> "Query":
        q = self._wrap() if self.sel is not None or self.keys is not None or self.rank_col else self
        return replace(q, sel=("*",) + tuple(f"{expr} AS {_q(alias)}" for alias, expr in named.items()))

    def _window(self, tvf: str) -> "Query":
        if not self._bare:
            raise BuilderError("windows must be applied directly to a source or model; move filters and "
                               "projections into an upstream model")
        return Query(from_=f"TABLE({tvf})", windowed=True, columns=None)

    def tumble(self, time_col: str, size) -> "Query":
        return self._window(f"TUMBLE(TABLE {self.base}, DESCRIPTOR({time_col}), {interval(size)})")

    def hop(self, time_col: str, slide, size) -> "Query":
        return self._window(f"HOP(TABLE {self.base}, DESCRIPTOR({time_col}), {interval(slide)}, {interval(size)})")

    def cumulate(self, time_col: str, step, size) -> "Query":
        return self._window(f"CUMULATE(TABLE {self.base}, DESCRIPTOR({time_col}), {interval(step)}, {interval(size)})")

    def group_by(self, *keys: str) -> "Grouped":
        q = self._wrap() if (self.sel is not None or self.keys is not None) else self
        if q.windowed:
            keys = tuple(k for k in ("window_start", "window_end") if k not in keys) + tuple(keys)
        return Grouped(q, tuple(keys))

    def _join(self, other, kind: str, on, left: str | None, right: str | None, extra: str = "") -> "Query":
        q = self._wrap() if not self._bare else self
        la = left or q._alias or q.base
        rq = other if isinstance(other, Query) else other._q()
        if not rq._bare:
            raise BuilderError("the right side of a join must be a source or model")
        ra = right or rq.base
        if la == ra:
            raise BuilderError(f"both join sides are aliased '{la}'; pass left=/right= aliases")
        if isinstance(on, (list, tuple)):
            cond = " AND ".join(f"{la}.{k} = {ra}.{k}" for k in on)
        else:
            cond = on
        frm = q.from_ if q._alias else (f"{q.base} AS {la}" if la != q.base else q.base)
        target = f"{rq.base}{extra} AS {ra}" if (ra != rq.base or extra) else rq.base
        return Query(from_=f"{frm}\n{kind} JOIN {target}\n  ON {cond}", joined=True)

    def join(self, other, on, how: str = "inner", within: tuple | None = None,
             left: str | None = None, right: str | None = None) -> "Query":
        """Regular join, or an interval join with within=(left_time, right_time, upper[, lower])."""
        kinds = {"inner": "INNER", "left": "LEFT", "right": "RIGHT", "full": "FULL OUTER"}
        if how not in kinds:
            raise BuilderError(f"how must be one of {', '.join(kinds)}")
        q = self
        if within is not None:
            if len(within) not in (3, 4):
                raise BuilderError("within=(left_time, right_time, upper[, lower])")
            lt, rt, upper = within[:3]
            lower = within[3] if len(within) == 4 else "0s"
            la = left or (self._alias or self.base)
            ra = right or (other.name if hasattr(other, "name") else other.base)
            on_sql = on if isinstance(on, str) else " AND ".join(f"{la}.{k} = {ra}.{k}" for k in on)
            low = f"{la}.{lt} - {interval(lower)}" if parse_duration_ms(lower) else f"{la}.{lt}"
            on = f"{on_sql}\n  AND {ra}.{rt} BETWEEN {low} AND {la}.{lt} + {interval(upper)}"
        return q._join(other, kinds[how], on, left, right)

    def temporal_join(self, versioned, on, as_of: str, how: str = "left",
                      left: str | None = None, right: str | None = None) -> "Query":
        """Join each row with the version of `versioned` valid at the row's `as_of` time."""
        la = left or (self._alias or self.base)
        kinds = {"inner": "INNER", "left": "LEFT"}
        if how not in kinds:
            raise BuilderError("temporal joins support how='inner' or 'left'")
        return self._join(versioned, kinds[how], on, left, right, extra=f" FOR SYSTEM_TIME AS OF {la}.{as_of}")

    def dedup(self, keys, order_by: str, keep: str = "first") -> "Query":
        """Keep the first (append-only when ordered by a time attribute) or last row per key."""
        if keep not in ("first", "last"):
            raise BuilderError("keep must be 'first' or 'last'")
        keys = [keys] if isinstance(keys, str) else list(keys)
        cols = self._output_columns()
        rn = (f"ROW_NUMBER() OVER (PARTITION BY {', '.join(keys)} ORDER BY {order_by} "
              f"{'ASC' if keep == 'first' else 'DESC'}) AS _rn")
        base = self._wrap() if self.keys is not None else self
        ranked = replace(base, sel=(base.sel or ("*",)) + (rn,))
        w = ranked._wrap()
        return replace(w, conds=("_rn = 1",), rank_col="_rn", columns=cols)

    def to_sql(self) -> str:
        if self.rank_col and self.sel is None:
            if not self.columns:
                raise BuilderError("call .select(...) after .dedup() to choose output columns "
                                   "(columns of this input are not known statically)")
            select = ", ".join(self.columns)
        else:
            select = ", ".join(self.sel) if self.sel else "*"
        sql = f"SELECT {select}\nFROM {self.from_}"
        if self.conds:
            sql += "\nWHERE " + "\n  AND ".join(self.conds)
        if self.keys:
            sql += "\nGROUP BY " + ", ".join(self.keys)
        return sql

    def __str__(self):
        return self.to_sql()


@dataclass(frozen=True)
class Grouped:
    query: Query
    keys: tuple

    def agg(self, **aggregates: str) -> Query:
        if not aggregates:
            raise BuilderError("agg() needs at least one aggregate, e.g. total=sum_('amount')")
        items = self.keys + tuple(f"{expr} AS {_q(alias)}" for alias, expr in aggregates.items())
        return replace(self.query, sel=items, keys=self.keys, base=None)
