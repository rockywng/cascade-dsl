"""Click sessions in Python: keyed state, event-time timers, late clicks to a side output.

A Python process model: `sessionize(ctx, inputs)` gets one PyFlink DataStream per input (keyed by
`key_by`) and returns a stream of rows matching the model's columns."""
from datetime import datetime, timedelta, timezone

from pyflink.common import Row, Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


def _time(millis: int) -> datetime:
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).replace(tzinfo=None)


def _parse_gap(text: str) -> int:
    """ISO-8601 minutes or seconds, as the Java example: PT30M, PT45S."""
    t = text.upper().removeprefix("PT")
    return int(t[:-1]) * (60_000 if t.endswith("M") else 1000)


class Sessions(KeyedProcessFunction):
    def __init__(self, gap_ms: int, late):
        self.gap = gap_ms
        self.late = late

    def open(self, ctx: RuntimeContext):
        self.start = ctx.get_state(ValueStateDescriptor("start", Types.LONG()))
        self.end = ctx.get_state(ValueStateDescriptor("end", Types.LONG()))
        self.count = ctx.get_state(ValueStateDescriptor("count", Types.LONG()))
        self.user = ctx.get_state(ValueStateDescriptor("user", Types.STRING()))

    def process_element(self, click, ctx: KeyedProcessFunction.Context):
        ts = ctx.timestamp()
        if ts <= ctx.timer_service().current_watermark():
            yield self.late, Row(user_id=click["user_id"], url=click["url"], ts=_time(ts))
            return
        start, end = self.start.value(), self.end.value()
        if end is not None:
            ctx.timer_service().delete_event_time_timer(end + self.gap)
            if ts > end + self.gap:  # the gap passed before the watermark did: close the old session
                yield self._session(end + self.gap)
                start = end = None
        self.start.update(ts if start is None else min(start, ts))
        self.end.update(ts if end is None else max(end, ts))
        self.count.update(1 if start is None else self.count.value() + 1)
        self.user.update(click["user_id"])
        ctx.timer_service().register_event_time_timer(self.end.value() + self.gap)

    def on_timer(self, t: int, ctx: KeyedProcessFunction.OnTimerContext):
        yield self._session(t)

    def _session(self, closed: int):
        row = Row(user_id=self.user.value(), session_start=_time(self.start.value()),
                  session_end=_time(self.end.value()), clicks=self.count.value(), session_closed=_time(closed))
        self.start.clear()
        self.end.clear()
        self.count.clear()
        self.user.clear()
        return row


def sessionize(ctx, inputs):
    clicks = inputs[0]  # keyed by user_id
    return (clicks.process(Sessions(_parse_gap(ctx.config.get("gap", "PT30M")), ctx.side_output("late")),
                           output_type=ctx.output_type)
            .uid(ctx.uid("sessionize")).name("sessionize"))
