"""Monitoring for a deployed pipeline: Prometheus alert rules and a Grafana dashboard, derived from the
pipeline (which sources lag, whether it has event time, which expectations drop rows).

Metric names are the Flink Prometheus reporter's, checked against a running job: integration/run.py
scrapes the compose cluster and fails if any metric an alert uses is missing.

    deploy:
      monitoring:
        alerts: {max_backpressure_ms: 500, max_pending_records: 100000, max_violation_ratio: 0.01}
"""
from __future__ import annotations

import json
import re

REPORTER = {"metrics.reporter.prom.factory.class": "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
            "metrics.reporter.prom.port": "9249"}
METRICS_PORT = 9249

RESTARTS = "flink_jobmanager_job_numRestarts"
UPTIME = "flink_jobmanager_job_uptime"
FAILED_CP = "flink_jobmanager_job_numberOfFailedCheckpoints"
LAST_CP = "flink_jobmanager_job_lastCheckpointCompletedTimestamp"
CP_DURATION = "flink_jobmanager_job_lastCheckpointDuration"
CP_SIZE = "flink_jobmanager_job_lastCheckpointSize"
BACKPRESSURE = "flink_taskmanager_job_task_backPressuredTimeMsPerSecond"
BUSY = "flink_taskmanager_job_task_busyTimeMsPerSecond"
RECORDS_IN = "flink_taskmanager_job_task_numRecordsInPerSecond"
PENDING = "flink_taskmanager_job_task_operator_pendingRecords"  # FLIP-33: Kafka sources
WATERMARK = "flink_taskmanager_job_task_currentInputWatermark"
EXPECT = "flink_taskmanager_job_task_operator_cascade_model_expectation_action_cascade_expectation"
# window aggregations count the rows they drop for arriving after the watermark passed their window
# (WindowAggOperator; interval joins and OVER aggregations drop late rows without a metric)
LATE = "flink_taskmanager_job_task_operator_numLateRecordsDropped"
# postgres_exporter (prometheus-community), replication_slot collector: checked by integration/connectors.py
SLOT_CURRENT = "pg_replication_slot_slot_current_wal_lsn"
SLOT_FLUSHED = "pg_replication_slot_slot_confirmed_flush_lsn"
SLOT_ACTIVE = "pg_replication_slot_slot_is_active"
SLOT_SAFE = "pg_replication_slot_safe_wal_size_bytes"


def enabled(cfg: dict) -> bool:
    m = cfg.get("monitoring")
    return bool(m) and m is not False


def settings(cfg: dict) -> dict:
    m = cfg.get("monitoring")
    alerts = (m.get("alerts") if isinstance(m, dict) else None) or {}
    return {"max_backpressure_ms": float(alerts.get("max_backpressure_ms", 500)),
            "max_pending_records": float(alerts.get("max_pending_records", 100000)),
            "max_violation_ratio": float(alerts.get("max_violation_ratio", 0.01)),
            "max_late_records": float(alerts.get("max_late_records", 0)),
            "max_slot_lag_bytes": float(alerts.get("max_slot_lag_bytes", 1 << 30)),
            "min_slot_safe_wal_bytes": float(alerts.get("min_slot_safe_wal_bytes", 1 << 30)),
            "labels": {str(k): str(v) for k, v in ((m.get("labels") if isinstance(m, dict) else None) or {}).items()}}


def _seconds(duration, default: int) -> int:
    from .loader import parse_duration_ms
    try:
        return max(1, parse_duration_ms(duration) // 1000) if duration else default
    except ValueError:
        return default


def rules(r, cfg: dict) -> dict:
    """Prometheus rule groups (the plain rule-file format; also the spec of a PrometheusRule)."""
    p = r.checked.pipeline
    s = settings(cfg)
    job = f'job_name="{p.name}"'
    interval = _seconds((cfg.get("checkpoints") or {}).get("interval"), 60)
    labels = {"pipeline": p.name, **s["labels"]}

    def alert(name, expr, for_, severity, summary):
        return {"alert": f"Cascade{name}", "expr": expr, "for": for_,
                "labels": {"severity": severity, **labels},
                "annotations": {"summary": summary, "pipeline": p.name}}

    out = [
        alert("JobDown", f"absent({UPTIME}{{{job}}})", "5m", "critical",
              f"{p.name}: no running job reports metrics"),
        alert("JobRestarting", f"increase({RESTARTS}{{{job}}}[10m]) > 0", "0m", "warning",
              f"{p.name}: the job restarted in the last 10 minutes"),
        alert("CheckpointsFailing", f"increase({FAILED_CP}{{{job}}}[15m]) > 0", "0m", "warning",
              f"{p.name}: checkpoints failed in the last 15 minutes"),
        alert("CheckpointsStalled", f"time() - max({LAST_CP}{{{job}}}) / 1000 > {interval * 5}", "2m", "critical",
              f"{p.name}: no checkpoint completed for 5 checkpoint intervals ({interval * 5}s); "
              "a restart would replay that much"),
        alert("Backpressure", f"max(avg_over_time({BACKPRESSURE}{{{job}}}[5m])) > {s['max_backpressure_ms']:g}",
              "10m", "warning", f"{p.name}: a task is backpressured more than {s['max_backpressure_ms']:g} ms/s"),
    ]
    from .connectors import spec_for
    kafka = [x.name for x in p.sources.values() if x.connection in p.connections
             and (spec_for(p.connections[x.connection].type, p.connections[x.connection]) or 0)
             and p.connections[x.connection].type == "kafka"]
    if kafka:
        out.append(alert("SourceLag", f"max({PENDING}{{{job}}}) > {s['max_pending_records']:g}", "10m", "warning",
                         f"{p.name}: Kafka sources ({', '.join(kafka)}) are more than "
                         f"{s['max_pending_records']:g} records behind"))
    if any(x.event_time for x in p.sources.values()):
        out.append(alert("EventTimeStalled", f"changes(max({WATERMARK}{{{job}}})[15m:1m]) == 0", "0m", "warning",
                         f"{p.name}: event time hasn't advanced for 15 minutes (windows don't close)"))
    windows = sorted(n for n, rel in p.relations.items() if any(op.kind == "window_agg" for op in rel.stateful_ops))
    if windows:
        out.append(alert("LateRecordsDropped", f"sum(increase({LATE}{{{job}}}[10m])) > {s['max_late_records']:g}",
                         "0m", "warning",
                         f"{p.name}: window aggregations ({', '.join(windows)}) dropped rows that arrived after their "
                         "window closed in the last 10 minutes; raise max_out_of_orderness if they matter"))
    if any(e.on_violation in ("drop", "quarantine") for m in p.models.values() for e in m.expectations):
        ratio = (f"sum by (cascade_model, expectation) (rate({EXPECT}_violations{{{job}}}[5m])) / "
                 f"sum by (cascade_model, expectation) (rate({EXPECT}_checked{{{job}}}[5m]))")
        out.append(alert("ExpectationViolations", f"{ratio} > {s['max_violation_ratio']:g}", "10m", "warning",
                         f"{p.name}: more than {s['max_violation_ratio']:.0%} of rows violate an expectation "
                         "({{ $labels.cascade_model }}.{{ $labels.expectation }})"))
    slots = postgres_slots(r)
    if slots:
        # needs postgres_exporter scraping the source database (it reads pg_replication_slots); the slot
        # name and the generations the runner moves it to on loss (<slot>_g<time>)
        sel = 'slot_name=~"' + "|".join(f"{s}(_g[0-9a-z]+)?" for s in slots) + '"'
        out += [
            alert("ReplicationSlotLag", f"max by (slot_name) ({SLOT_CURRENT}{{{sel}}} - {SLOT_FLUSHED}{{{sel}}}) "
                  f"> {s['max_slot_lag_bytes']:g}", "10m", "warning",
                  f"{p.name}: replication slot {{{{ $labels.slot_name }}}} is more than "
                  f"{s['max_slot_lag_bytes']:g} bytes behind (Postgres keeps that WAL until it catches up)"),
            alert("ReplicationSlotInactive", f"max by (slot_name) ({SLOT_ACTIVE}{{{sel}}}) == 0", "10m", "warning",
                  f"{p.name}: nothing reads replication slot {{{{ $labels.slot_name }}}}"),
            alert("ReplicationSlotAtRisk", f"min by (slot_name) ({SLOT_SAFE}{{{sel}}}) "
                  f"< {s['min_slot_safe_wal_bytes']:g}", "5m", "critical",
                  f"{p.name}: replication slot {{{{ $labels.slot_name }}}} is close to max_slot_wal_keep_size; "
                  "if Postgres invalidates it, changes are lost"),
            alert("ReplicationSlotGone", f"absent({SLOT_CURRENT}{{{sel}}})", "10m", "critical",
                  f"{p.name}: no replication slot for its Postgres CDC sources ({', '.join(slots)}): it was dropped "
                  "or invalidated; the next restore refuses or re-snapshots (on_slot_loss)"),
        ]
    return {"groups": [{"name": f"cascade-{p.name}", "rules": out}]}


def postgres_slots(r) -> list[str]:
    p = r.checked.pipeline
    if p.replicate is not None:  # a replication's Postgres slot (replicate.py)
        conn = p.connections.get(str(p.replicate.get("from")))
        if conn is None or conn.type != "postgres-cdc":
            return []
        from .replicate import slot_name
        return [p.replicate.get("slot_name") or slot_name(p)]
    opts = r.checked.options
    return sorted({o["slot.name"] for o in opts.values() if "slot.name" in o
                   and "postgres-cdc" in (o.get("connector"), o.get("delegate"))})


def metric_names(rule_groups: dict) -> set[str]:
    names = set()
    for g in rule_groups["groups"]:
        for rule in g["rules"]:
            names |= set(re.findall(r"\b((?:flink|pg)_[A-Za-z0-9_]+)", rule["expr"]))
    return names


def dashboard(r, cfg: dict) -> dict:
    p = r.checked.pipeline
    job = f'job_name="{p.name}"'
    panels = [
        ("Uptime (s)", f"max({UPTIME}{{{job}}}) / 1000", "s"),
        ("Restarts", f"max({RESTARTS}{{{job}}})", "short"),
        ("Records in / s by task", f"sum by (task_name) ({RECORDS_IN}{{{job}}})", "short"),
        ("Busy ms/s by task", f"max by (task_name) ({BUSY}{{{job}}})", "ms"),
        ("Backpressured ms/s by task", f"max by (task_name) ({BACKPRESSURE}{{{job}}})", "ms"),
        ("Checkpoint duration", f"max({CP_DURATION}{{{job}}})", "ms"),
        ("Checkpoint size", f"max({CP_SIZE}{{{job}}})", "bytes"),
        ("Failed checkpoints", f"max({FAILED_CP}{{{job}}})", "short"),
        ("Kafka records pending by source", f"sum by (operator_name) ({PENDING}{{{job}}})", "short"),
        ("Event-time lag (s)", f"time() - max({WATERMARK}{{{job}}}) / 1000", "s"),
        ("Expectation violations / s", f"sum by (cascade_model, expectation) "
                                        f"(rate({EXPECT}_violations{{{job}}}[5m]))", "short"),
    ]
    out = []
    for i, (title, expr, unit) in enumerate(panels):
        out.append({"id": i + 1, "type": "timeseries", "title": title,
                    "gridPos": {"h": 8, "w": 12, "x": (i % 2) * 12, "y": (i // 2) * 8},
                    "datasource": {"type": "prometheus", "uid": "${datasource}"},
                    "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
                    "targets": [{"expr": expr, "refId": "A", "legendFormat": "__auto"}]})
    return {"title": f"cascade: {p.name}", "uid": f"cascade-{p.name}"[:40], "schemaVersion": 39,
            "time": {"from": "now-6h", "to": "now"}, "tags": ["cascade", "flink"],
            "templating": {"list": [{"name": "datasource", "type": "datasource", "query": "prometheus"}]},
            "panels": out}


def kubernetes(r, cfg: dict, name: str, metadata) -> dict[str, dict]:
    """PodMonitor, PrometheusRule and a Grafana dashboard ConfigMap (sidecar label grafana_dashboard)."""
    return {
        "podmonitor.yaml": {
            "apiVersion": "monitoring.coreos.com/v1", "kind": "PodMonitor", "metadata": metadata(f"{name}-metrics"),
            "spec": {"selector": {"matchLabels": {"app": name}},
                     "podMetricsEndpoints": [{"port": "metrics", "interval": "30s"}]}},
        "prometheusrule.yaml": {
            "apiVersion": "monitoring.coreos.com/v1", "kind": "PrometheusRule", "metadata": metadata(f"{name}-alerts"),
            "spec": rules(r, cfg)},
        "grafana-dashboard.yaml": {
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {**metadata(f"{name}-dashboard"),
                         "labels": {**metadata(f"{name}-dashboard")["labels"], "grafana_dashboard": "1"}},
            "data": {f"cascade-{r.checked.pipeline.name}.json": json.dumps(dashboard(r, cfg), indent=1)}},
    }
