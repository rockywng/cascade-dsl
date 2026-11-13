"""`cascade deploy`: Kubernetes resources for the Flink Kubernetes Operator.

Output (one directory per pipeline/environment, ready for kubectl apply -k or GitOps):

  flinkdeployment.yaml   FlinkDeployment running dev.cascade.runner.SqlRunner from the runtime image
  configmap.yaml         the job: setup.sql + plan.json (EXECUTE PLAN, pinned UIDs) or statements.sql
  secret.template.yaml   the Kubernetes Secret the job expects, keys listed, values left empty
  kustomization.yaml     lists the above (the Secret template is not included)
  Dockerfile             runtime image: Flink + connector jars from the manifest + cascade-runtime.jar
  operators.json         copied from the plan directory when given (vertex -> model mapping)

Secrets: every `${secret:a/b}` stays a placeholder in the ConfigMap. The FlinkDeployment maps
env var CASCADE_SECRET_A_B from key `a.b` of the deploy Secret, and the runner substitutes it into
the SQL at startup. Nothing secret is written to disk.
"""
from __future__ import annotations

import json
import os
import re
import shutil

import yaml

from . import __version__
from .compiler import Result
from .connectors import PATCHES, TESTED_VERSIONS
from .jvm import script
from .lower import Stmt
from .udfs import PYRUNNER, needs_python, python_files, python_functions, python_processes

RUNNER = "dev.cascade.runner.SqlRunner"
PYTHON_DRIVER = "org.apache.flink.client.python.PythonDriver"
JOB_DIR = "/opt/cascade/job"
RUNTIME_JAR = "/opt/flink/usrlib/cascade-runtime.jar"
USRLIB = "/opt/flink/usrlib"
PYTHON_DIR = "/opt/cascade/python"
PYFLINK_PYTHON = "/opt/pyflink/bin/python"
DEFAULT_FLINK = "2.3.0"
SECRET_RE = re.compile(r"\$\{secret:([^}]+)\}")


class DeployError(ValueError):
    pass


def env_name(secret: str) -> str:
    """Must match SqlRunner.envName."""
    return "CASCADE_SECRET_" + re.sub(r"[^A-Za-z0-9]", "_", secret.strip()).upper()


def secret_key(secret: str) -> str:
    """Key in the Kubernetes Secret (allowed: alphanumerics, '-', '_', '.')."""
    return re.sub(r"[^A-Za-z0-9_.-]", "-", secret.strip().replace("/", "."))


def resource_name(pipeline: str, env: str | None) -> str:
    name = pipeline + (f"-{env}" if env else "")
    name = re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-")
    return name[:45].rstrip("-")


def _memory(value) -> str:
    """'4g' / '512m' / '2Gi' -> Kubernetes-style quantity the operator accepts."""
    s = str(value).strip()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([gGmM])(?:i?[bB]?)?", s)
    if not m:
        return s
    return f"{m.group(1)}{'g' if m.group(2).lower() == 'g' else 'm'}"


def _operator_version(flink: str) -> str:
    major, minor = flink.split(".")[:2]
    return f"v{major}_{minor}"


class _Dumper(yaml.SafeDumper):
    pass


def _str_presenter(dumper, data):
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


_Dumper.add_representer(str, _str_presenter)


def _dump(obj) -> str:
    return yaml.dump(obj, Dumper=_Dumper, sort_keys=False, default_flow_style=False, width=120)


def build(r: Result, out: str, plan_dir: str | None = None, image: str | None = None) -> dict:
    """Write the deployment directory. Returns a summary (name, mode, secrets, files)."""
    c, lowered = r.checked, r.lowered
    p = c.pipeline
    cfg = dict(p.deploy or {})
    target = str(cfg.get("target", "kubernetes"))
    if target not in TARGETS:
        raise DeployError(f"unknown deploy target '{target}' (one of: {', '.join(TARGETS)})")
    image = image or cfg.get("image")
    if target == "kubernetes" and not image:
        raise DeployError("no runtime image: set `deploy.image` or pass --image")
    name = resource_name(p.name, p.env)
    secrets = sorted(p.secrets)
    by_env: dict[str, str] = {}
    for s in secrets:
        if env_name(s) in by_env:
            raise DeployError(f"secrets '{by_env[env_name(s)]}' and '{s}' map to the same variable {env_name(s)}")
        by_env[env_name(s)] = s
    secret_name = cfg.get("secrets") or f"{name}-secrets"

    os.makedirs(out, exist_ok=True)
    rewrite = _in_image if target != "flink-run" else (lambda st: _job_local(st, out))
    keep_guard = _guard_filter(plan_dir)
    setup = [rewrite(st) for st in lowered.statements if st.kind != "insert" and (st.kind != "guard" or keep_guard(st))]
    files = {"setup.sql": _header(r, "setup") + script(setup)}
    if plan_dir and any(m.process for m in p.models.values()):
        raise DeployError("pipelines with process models deploy as statements (a compiled plan can't contain "
                          "DataStream code); drop --plan-dir")
    if plan_dir and p.mode == "batch":
        raise DeployError("batch pipelines deploy as statements (Flink compiles plans for streaming jobs only); "
                          "drop --plan-dir")
    if plan_dir:
        plan_path = os.path.join(plan_dir, "plan.json")
        if not os.path.exists(plan_path):
            raise DeployError(f"{plan_dir} has no plan.json (create it with `cascade plan`)")
        with open(plan_path) as f:
            plan = json.load(f)
        _check_plan_matches(plan, lowered.statements)
        files["plan.json"] = json.dumps(plan, indent=1)
        args = ["--setup", f"{JOB_DIR}/setup.sql", "--plan", f"{JOB_DIR}/plan.json"]
        mode = "plan"
        ops = os.path.join(plan_dir, "operators.json")
        if os.path.exists(ops):
            shutil.copy(ops, os.path.join(out, "operators.json"))
    else:
        inserts = [st for st in lowered.statements if st.kind == "insert"]
        files["statements.sql"] = _header(r, "statements") + script(inserts)
        args = ["--setup", f"{JOB_DIR}/setup.sql", "--statements", f"{JOB_DIR}/statements.sql"]
        mode = "statements"

    if target == "flink-run":
        return _flink_run(r, out, files, mode, secrets, cfg)

    labels = {"app.kubernetes.io/name": name, "app.kubernetes.io/managed-by": "cascade",
              "cascade.dev/pipeline": p.name, **({"cascade.dev/environment": p.env} if p.env else {}),
              **{str(k): str(v) for k, v in (cfg.get("labels") or {}).items()}}
    namespace = cfg.get("namespace")
    meta = lambda n: {"name": n, **({"namespace": namespace} if namespace else {}), "labels": labels}  # noqa: E731

    # ConfigMaps are named after their content: a running job never sees the next version's files
    # (Kubernetes updates mounted ConfigMaps in place), and a new version changes the pod template,
    # which is what makes the operator perform a savepoint upgrade.
    job_cm = f"{name}-job-{_content_hash(files)}"
    py_files = python_files(p) if needs_python(p) else None
    py_cm_name = f"{name}-python-{_content_hash(py_files)}" if py_files else None
    if target == "session":
        return _session_job(r, out, name, meta(name), files, mode, secrets, cfg)
    configmap = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta(job_cm), "data": files}
    deployment = _flink_deployment(r, name, meta(name), image, args, secret_name, cfg, job_cm, py_cm_name)
    resources = ["configmap.yaml", "flinkdeployment.yaml"]
    if py_files:
        py_cm = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta(py_cm_name),
                 "data": py_files}
        with open(os.path.join(out, "python-configmap.yaml"), "w") as f:
            f.write(_dump(py_cm))
        resources.insert(1, "python-configmap.yaml")
    secret = {"apiVersion": "v1", "kind": "Secret", "metadata": meta(secret_name), "type": "Opaque",
              "stringData": {secret_key(s): "" for s in secrets}}
    kustomization = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
                     "resources": resources}

    def write(fname, text):
        with open(os.path.join(out, fname), "w") as f:
            f.write(text)

    write("configmap.yaml", _dump(configmap))
    write("flinkdeployment.yaml", _dump(deployment))
    store = cfg.get("secrets_from")
    if secrets and store:
        # External Secrets Operator: the Secret is synced from Vault / AWS Secrets Manager / GCP ...
        es = _external_secret(store, secret_name, secrets, meta)
        write("externalsecret.yaml", _dump(es))
        kustomization["resources"].append("externalsecret.yaml")
    elif secrets:
        write("secret.template.yaml", "# Fill in and apply separately (or create it from your secret manager).\n"
              "# Never commit real values.\n" + _dump(secret))
    from . import monitoring
    if monitoring.enabled(cfg):
        for fname, doc in monitoring.kubernetes(r, cfg, name, meta).items():
            write(fname, _dump(doc))
            kustomization["resources"].append(fname)
        _monitoring_files(r, cfg, out)
    write("kustomization.yaml", _dump(kustomization))
    write("Dockerfile", dockerfile(r, cfg))
    return {"name": name, "mode": mode, "secrets": secrets, "secret_name": secret_name, "out": out}


TARGETS = ("kubernetes", "session", "flink-run")


def _guard_filter(plan_dir: str | None):
    """Replication-slot guards apply to sources that resume from the savepoint. `cascade plan
    --previous` knows which those are; a source new in this version has no slot yet."""
    path = os.path.join(plan_dir, "restore.json") if plan_dir else None
    if not path or not os.path.exists(path):
        return lambda st: True
    with open(path) as f:
        info = json.load(f)
    if not info.get("previous"):
        return lambda st: True  # a first plan: nothing restores, the guards stay inert
    restored = set(info.get("restored_sources") or [])
    return lambda st: st.name in restored


def _job_args(files: dict, mode: str, inline: bool) -> list[str]:
    """SqlRunner arguments: files next to the runner, or the scripts themselves (base64) when the
    job can't be given files (a FlinkSessionJob runs on a shared cluster)."""
    import base64
    second = "plan" if mode == "plan" else "statements"
    fname = "plan.json" if mode == "plan" else "statements.sql"
    if inline:
        enc = lambda t: base64.b64encode(t.encode()).decode()  # noqa: E731
        return ["--setup-b64", enc(files["setup.sql"]), f"--{second}-b64", enc(files[fname])]
    return ["--setup", "job/setup.sql", f"--{second}", f"job/{fname}"]


def _external_secret(store: dict, secret_name: str, secrets: list, meta) -> dict:
    """An ExternalSecret (external-secrets.io) that fills the job's Secret from a secret store.

    deploy.secrets_from: {store: vault-backend, kind: ClusterSecretStore, prefix: "pipelines/orders/",
                          refresh: 1h}
    Each `${secret:a/b}` is read from remote key `<prefix>a/b` (property: the last path segment when
    `property_from_key: true`)."""
    if not isinstance(store, dict) or not store.get("store"):
        raise DeployError("deploy.secrets_from needs `store` (the name of a SecretStore or ClusterSecretStore)")
    prefix = str(store.get("prefix", ""))
    data = []
    for s in secrets:
        ref = {"key": prefix + s}
        if store.get("property_from_key"):
            path, _, prop = s.rpartition("/")
            ref = {"key": prefix + path, "property": prop}
        data.append({"secretKey": secret_key(s), "remoteRef": ref})
    return {"apiVersion": "external-secrets.io/v1", "kind": "ExternalSecret", "metadata": meta(secret_name),
            "spec": {"refreshInterval": str(store.get("refresh", "1h")),
                     "secretStoreRef": {"name": str(store["store"]), "kind": str(store.get("kind", "SecretStore"))},
                     "target": {"name": secret_name, "creationPolicy": "Owner"},
                     "data": data}}


def _monitoring_files(r: Result, cfg: dict, out: str):
    """Plain Prometheus rule file and Grafana dashboard JSON, for any target."""
    from . import monitoring
    d = os.path.join(out, "monitoring")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "alerts.yml"), "w") as f:
        f.write(_dump(monitoring.rules(r, cfg)))
    with open(os.path.join(d, "dashboard.json"), "w") as f:
        json.dump(monitoring.dashboard(r, cfg), f, indent=1)


def _session_job(r: Result, out: str, name: str, metadata: dict, files: dict, mode: str, secrets: list,
                 cfg: dict) -> dict:
    """A FlinkSessionJob on an existing (operator-managed) session cluster."""
    p = r.checked.pipeline
    cluster = cfg.get("session_cluster")
    jar = cfg.get("jar_uri")
    if not cluster or not jar:
        raise DeployError("target: session needs deploy.session_cluster (the FlinkDeployment running the session "
                          "cluster) and deploy.jar_uri (where the operator fetches cascade-runtime.jar: https://, "
                          "s3://, ...)")
    from .udfs import needs_python
    if needs_python(p):
        raise DeployError("Python UDFs need their files on the cluster; a session job can't ship them. "
                          "Use target: kubernetes")
    sc = p.scaling
    job = {
        "apiVersion": "flink.apache.org/v1beta1", "kind": "FlinkSessionJob", "metadata": metadata,
        "spec": {
            "deploymentName": cluster,
            "job": {"jarURI": jar, "entryClass": RUNNER, "args": _job_args(files, mode, inline=True),
                    "parallelism": (sc.parallelism if sc and sc.parallelism else 1),
                    "upgradeMode": cfg.get("upgrade_mode", "stateless" if p.mode == "batch" else "savepoint"),
                    "state": "running"},
            **({"flinkConfiguration": {str(k): str(v) for k, v in (cfg.get("flink_config") or {}).items()}}
               if cfg.get("flink_config") else {}),
        },
    }
    with open(os.path.join(out, "flinksessionjob.yaml"), "w") as f:
        f.write(_dump(job))
    from . import monitoring
    if monitoring.enabled(cfg):
        _monitoring_files(r, cfg, out)
    notes = ["# Session cluster requirements for this job (it shares the cluster's image and pods):",
             *([f"#   the operator fetches {jar}: it allows only https by default; for another scheme add it to "
                "kubernetes.operator.user.artifacts.allowed-schemes (operator configuration; a list separated by ;, e.g. https;http)"]
               if not str(jar).startswith("https://") else []),
             "#   the jar host must resolve to a public address: the operator refuses private, loopback and link-"
             "local hosts (kubernetes.operator.user.artifacts.disallow-restricted-hosts, SSRF protection)",
             "#   connector jars: " + (", ".join(sorted(r.lowered.manifest["artifacts"])) or "none"),
             "#   secrets as environment variables on the JobManager and TaskManagers: "
             + (", ".join(env_name(s) for s in secrets) or "none")]
    jars = [m.process["jar"] for m in p.models.values() if m.process and m.process.get("jar")]
    jars += [f.jar for f in p.functions.values() if f.language == "java" and f.jar]
    if jars:
        notes.append("#   in the image's usrlib: " + ", ".join(sorted({os.path.basename(j) for j in jars})))
    if any(k.connection in p.connections and p.connections[k.connection].type == "jdbc" for k in p.sinks.values()):
        notes.append("#   WARNING: a JDBC sink's database outage shuts down the whole TaskManager (CD215): every "
                     "job on it fails over")
    with open(os.path.join(out, "REQUIREMENTS"), "w") as f:
        f.write("\n".join(notes) + "\n")
    with open(os.path.join(out, "kustomization.yaml"), "w") as f:
        f.write(_dump({"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
                       "resources": ["flinksessionjob.yaml"]}))
    return {"name": name, "mode": mode, "secrets": secrets, "secret_name": None, "out": out, "target": "session"}


def _job_local(st: Stmt, out: str) -> Stmt:
    """flink-run: copy the job's jars and Python files into out/job and refer to them relatively."""
    import shutil as _sh
    job = os.path.join(out, "job")
    os.makedirs(job, exist_ok=True)
    if st.kind == "function" and st.options.get("jar"):
        jar = st.options["jar"]
        _sh.copy(jar, job)
        target = f"job/{os.path.basename(jar)}"
        return Stmt(st.kind, st.name, st.sql.replace(_sql_lit(jar), _sql_lit(target)), st.comment,
                    options={"jar": target})
    if st.kind == "set" and st.name == "python.files":
        _sh.copytree(st.options[st.name], os.path.join(job, "python"), dirs_exist_ok=True)
        return Stmt("set", st.name, "SET 'python.files' = 'job/python';", options={st.name: "job/python"})
    if st.kind == "process":
        from .lower import PROCESS_DIRECTIVE
        spec = json.loads(st.sql[len(PROCESS_DIRECTIVE):])
        if spec.get("jar"):
            _sh.copy(spec["jar"], job)
            spec["jar"] = f"job/{os.path.basename(spec['jar'])}"
        if spec.get("file"):  # a Python process model, next to the Python runner
            _sh.copy(spec["file"], job)
            _sh.copy(os.path.join(os.path.dirname(__file__), "pyrunner.py"), os.path.join(job, PYRUNNER))
            spec["file"] = f"job/{os.path.basename(spec['file'])}"
        return Stmt(st.kind, st.name, f"{PROCESS_DIRECTIVE} {json.dumps(spec, sort_keys=True)}", st.comment)
    return st


def _flink_run(r: Result, out: str, files: dict, mode: str, secrets: list, cfg: dict) -> dict:
    """flink-run: a run.sh around `flink run`, for standalone/remote session clusters (the client runs
    the job's main and submits the job graph) and YARN application mode (the job dir is shipped)."""
    run = cfg.get("run") or {}
    how = str(run.get("mode", "remote"))
    if how not in ("remote", "yarn-application"):
        raise DeployError("deploy.run.mode must be remote or yarn-application")
    job = os.path.join(out, "job")
    os.makedirs(job, exist_ok=True)
    for fname, text in files.items():
        with open(os.path.join(job, fname), "w") as f:
            f.write(text)
    args = " ".join(_job_args(files, mode, inline=False))
    opts = ["-d"] + [f"-D{k}={v}" for k, v in sorted((cfg.get("flink_config") or {}).items())]  # detached
    if how == "yarn-application":
        opts = ["-t yarn-application", "-Dyarn.ship-files=job"] + opts[1:]  # application mode is detached
    elif run.get("jobmanager"):
        opts = [f"-m {run['jobmanager']}"] + opts
    lines = ["#!/usr/bin/env bash",
             f"# Submit {r.checked.pipeline.name} with `flink run` ({how}). Generated by cascade {__version__}.",
             "# Needs: FLINK_HOME (a Flink distribution whose lib/ has the connector jars:",
             "#   " + (", ".join(sorted(r.lowered.manifest["artifacts"])) or "none") + ")",
             "#   CASCADE_RUNTIME_JAR (runtime/target/cascade-runtime.jar)",
             ("#   secrets in the environment: " + ", ".join(env_name(s) for s in secrets)) if secrets else
             "#   (no secrets)",
             "set -euo pipefail",
             'cd "$(dirname "$0")"',
             ': "${FLINK_HOME:?set FLINK_HOME}"',
             ': "${CASCADE_RUNTIME_JAR:?set CASCADE_RUNTIME_JAR to cascade-runtime.jar}"',
             (f'exec "$FLINK_HOME/bin/flink" run {" ".join(opts)} -py job/{PYRUNNER} '
              f'-pyfs "$CASCADE_RUNTIME_JAR" job {args} "$@"' if python_processes(r.checked.pipeline) else
              f'exec "$FLINK_HOME/bin/flink" run {" ".join(opts)} -c {RUNNER} "$CASCADE_RUNTIME_JAR" {args} "$@"')
             .replace("  ", " ")]
    from . import monitoring
    if monitoring.enabled(cfg):
        _monitoring_files(r, cfg, out)
    path = os.path.join(out, "run.sh")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    os.chmod(path, 0o755)
    return {"name": r.checked.pipeline.name, "mode": mode, "secrets": secrets, "secret_name": None, "out": out,
            "target": "flink-run"}


def _in_image(st: Stmt) -> Stmt:
    """Rewrite local paths in setup statements to where the image / pod has the files."""
    if st.kind == "function" and st.options.get("jar"):
        jar = st.options["jar"]
        target = f"{USRLIB}/{os.path.basename(jar)}"
        return Stmt(st.kind, st.name, st.sql.replace(_sql_lit(jar), _sql_lit(target)), st.comment,
                    options={"jar": target})
    if st.kind == "set" and st.name == "python.files":
        return Stmt("set", st.name, f"SET 'python.files' = '{PYTHON_DIR}';", options={st.name: PYTHON_DIR})
    if st.kind == "process":
        from .lower import PROCESS_DIRECTIVE
        spec = json.loads(st.sql[len(PROCESS_DIRECTIVE):])
        if spec.get("jar"):
            spec["jar"] = f"{USRLIB}/{os.path.basename(spec['jar'])}"
        if spec.get("file"):  # a Python process model: shipped in the Python ConfigMap
            spec["file"] = f"{PYTHON_DIR}/{os.path.basename(spec['file'])}"
        return Stmt(st.kind, st.name, f"{PROCESS_DIRECTIVE} {json.dumps(spec, sort_keys=True)}", st.comment)
    return st


def _sql_lit(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def _header(r: Result, what: str) -> str:
    p = r.checked.pipeline
    return (f"-- {what} for pipeline {p.name}" + (f" (env: {p.env})" if p.env else "")
            + f", generated by cascade {__version__}; sql sha256 {r.lowered.manifest['sql_sha256'][:16]}\n")


def _check_plan_matches(plan: dict, stmts: list[Stmt]):
    """The plan must read and write exactly the tables this compilation defines."""
    tables = {st.name for st in stmts if st.kind in ("source", "sink", "quarantine")}
    used = set()
    for n in plan.get("nodes", []):
        for key in ("scanTableSource", "dynamicTableSink", "lookupTableSource"):
            ident = (n.get(key) or {}).get("table", {}).get("identifier")
            if ident:
                used.add(ident.replace("`", "").split(".")[-1])
        ident = ((n.get("temporalTable") or {}).get("lookupTableSource") or {}).get("table", {}).get("identifier")
        if ident:
            used.add(ident.replace("`", "").split(".")[-1])
    unknown = used - tables
    if unknown:
        raise DeployError(f"plan.json references tables this pipeline does not define: {sorted(unknown)}; "
                          "recompile it with `cascade plan` for this version/environment")


def merge_pod_template(base: dict, extra: dict) -> dict:
    """deploy.pod_template merged into the generated one: mappings merge recursively, containers
    merge by name, other lists are appended (volumes, tolerations, ...)."""
    out = dict(base)
    for k, v in extra.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = merge_pod_template(out[k], v)
        elif k == "containers" and isinstance(v, list):
            by_name = {c.get("name"): dict(c) for c in out.get(k, [])}
            for c in v:
                name = c.get("name")
                by_name[name] = merge_pod_template(by_name[name], c) if name in by_name else dict(c)
            out[k] = list(by_name.values())
        elif k in out and isinstance(out[k], list) and isinstance(v, list):
            out[k] = out[k] + v
        else:
            out[k] = v
    return out


def _content_hash(files: dict) -> str:
    import hashlib
    return hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()[:10]


def _flink_deployment(r: Result, name: str, metadata: dict, image: str, args: list[str], secret_name: str,
                      cfg: dict, job_cm: str, py_cm: str | None) -> dict:
    p = r.checked.pipeline
    sc = p.scaling
    conf: dict[str, str] = {}
    cp = cfg.get("checkpoints") or {}
    conf["execution.checkpointing.interval"] = str(cp.get("interval", "1m"))
    if cp.get("mode"):
        conf["execution.checkpointing.mode"] = str(cp["mode"]).upper()
    if cp.get("timeout"):
        conf["execution.checkpointing.timeout"] = str(cp["timeout"])
    if cp.get("min_pause"):
        conf["execution.checkpointing.min-pause"] = str(cp["min_pause"])
    if cp.get("dir"):
        conf["execution.checkpointing.dir"] = str(cp["dir"])
    if cfg.get("savepoints"):
        conf["execution.checkpointing.savepoint-dir"] = str(cfg["savepoints"])
    conf["state.backend.type"] = "rocksdb"
    conf["execution.checkpointing.incremental"] = "true"
    tm = cfg.get("taskmanager") or {}
    conf["taskmanager.numberOfTaskSlots"] = str(tm.get("slots", 1))
    if sc and sc.max_parallelism:
        conf["pipeline.max-parallelism"] = str(sc.max_parallelism)
    if sc and sc.autoscale and (sc.min or sc.max or sc.target_utilization):
        conf["job.autoscaler.enabled"] = "true"
        conf["job.autoscaler.scaling.enabled"] = "true"
        if sc.target_utilization:
            conf["job.autoscaler.target.utilization"] = str(sc.target_utilization)
        if sc.min:
            conf["job.autoscaler.vertex.min-parallelism"] = str(sc.min)
        if sc.max:
            conf["job.autoscaler.vertex.max-parallelism"] = str(sc.max)
        if sc.max_lag_ms:
            # catch up within the lag budget after a rescale
            conf["job.autoscaler.catch-up.duration"] = f"{max(sc.max_lag_ms // 1000, 1)}s"
    if needs_python(p):
        conf["python.executable"] = PYFLINK_PYTHON
        conf["python.client.executable"] = PYFLINK_PYTHON
    from . import monitoring
    if monitoring.enabled(cfg):
        conf.update(monitoring.REPORTER)
    conf.update({str(k): str(v) for k, v in (cfg.get("flink_config") or {}).items()})

    jm = cfg.get("jobmanager") or {}
    env = [{"name": env_name(s), "valueFrom": {"secretKeyRef": {"name": secret_name, "key": secret_key(s)}}}
           for s in sorted(p.secrets)]
    container = {"name": "flink-main-container",
                 "volumeMounts": [{"name": "cascade-job", "mountPath": JOB_DIR, "readOnly": True}]}
    if monitoring.enabled(cfg):
        container["ports"] = [{"name": "metrics", "containerPort": monitoring.METRICS_PORT}]
    volumes = [{"name": "cascade-job", "configMap": {"name": job_cm}}]
    if needs_python(p):
        container["volumeMounts"].append({"name": "cascade-python", "mountPath": PYTHON_DIR, "readOnly": True})
        volumes.append({"name": "cascade-python", "configMap": {"name": py_cm}})
    if env:
        container["env"] = env
    annotations = {"cascade.dev/compiler": f"cascade {__version__}",
                   "cascade.dev/sql-sha256": r.lowered.manifest["sql_sha256"]}
    if sc and sc.max_lag_ms:
        annotations["cascade.dev/max-lag-ms"] = str(sc.max_lag_ms)
    if sc and any(m.scaling for m in p.models.values()):
        annotations["cascade.dev/model-scaling"] = json.dumps(
            {m.name: {"min": m.scaling.min, "max": m.scaling.max} for m in p.models.values() if m.scaling},
            sort_keys=True)
    pod_template = {
        "metadata": {"annotations": {"cascade.dev/job-config": job_cm}},
        "spec": {"containers": [container], "volumes": volumes},
    }
    if cfg.get("pod_template"):
        pod_template = merge_pod_template(pod_template, cfg["pod_template"])
    return {
        "apiVersion": "flink.apache.org/v1beta1",
        "kind": "FlinkDeployment",
        "metadata": {**metadata, "annotations": annotations},
        "spec": {
            "image": image,
            "flinkVersion": _operator_version(str(cfg.get("flink_version", DEFAULT_FLINK))),
            "serviceAccount": cfg.get("service_account", "flink"),
            "flinkConfiguration": conf,
            "jobManager": {"replicas": int(jm.get("replicas", 1)),
                           "resource": {"memory": _memory(jm.get("memory", "2g")), "cpu": float(jm.get("cpu", 1))}},
            "taskManager": {"resource": {"memory": _memory(tm.get("memory", "4g")), "cpu": float(tm.get("cpu", 1))}},
            "podTemplate": pod_template,
            "job": {
                # Python process models: the job is driven from Python (cascade_dsl/pyrunner.py)
                "jarURI": f"local:///opt/flink/opt/flink-python-{cfg.get('flink_version', DEFAULT_FLINK)}.jar"
                if python_processes(p) else f"local://{RUNTIME_JAR}",
                "entryClass": PYTHON_DRIVER if python_processes(p) else RUNNER,
                "args": (["-py", f"{PYTHON_DIR}/{PYRUNNER}", "job", *args]) if python_processes(p) else args,
                "parallelism": (sc.parallelism if sc and sc.parallelism else 1),
                # a batch run has no state to carry over; a new spec reruns the job
                "upgradeMode": cfg.get("upgrade_mode", "stateless" if p.mode == "batch" else "savepoint"),
                "state": "running",
            },
        },
    }


def dockerfile(r: Result, cfg: dict) -> str:
    """Runtime image: Flink, the connector jars the pipeline needs, the cascade runtime and UDF jars."""
    flink = str(cfg.get("flink_version", DEFAULT_FLINK))
    versions = {str(k): str(v) for k, v in (cfg.get("artifacts") or {}).items()}
    lines = [f"# Runtime image for {r.checked.pipeline.name}, generated by cascade {__version__}.",
             "# Build from the repository root after runtime/build.sh:",
             "#   docker build -f <this file> --build-arg ... -t <image> .",
             "# On the platform, BASE is the Cascade Flink distribution (cascade-dataplane images/flink),",
             "# which adds the S3 filesystems that checkpoints and savepoints on s3:// need.",
             f"ARG BASE=flink:{flink}-scala_2.12-java17"]
    flink_minor = ".".join(flink.split(".")[:2])
    patched = {}  # coord -> (arg, default, jar file name)
    for coord in r.lowered.manifest["artifacts"]:
        coord = coord.format(flink_minor=flink_minor)
        group, artifact = coord.split(":")[:2]
        if f"{group}:{artifact}" in PATCHES:
            arg = re.sub(r"[^A-Z0-9]", "_", artifact.upper()) + "_VERSION"
            default = versions.get(f"{group}:{artifact}") or TESTED_VERSIONS.get(f"{group}:{artifact}")
            patched[f"{group}:{artifact}"] = (arg, default)
    if patched:
        # a JDK stage fixes connector jars that are broken as released (connectors.PATCHES)
        lines += ["", "FROM eclipse-temurin:17-jdk AS cascade-patches", "WORKDIR /patches"]
        for coord, (arg, default) in patched.items():
            group, artifact = coord.split(":")
            lines.append(f"ARG {arg}" + (f"={default}" if default else ""))
            url = (f"https://repo1.maven.org/maven2/{group.replace('.', '/')}/{artifact}/${{{arg}}}/"
                   f"{artifact}-${{{arg}}}.jar")
            jar = f"{artifact}-${{{arg}}}.jar"
            lines.append(f'ADD "{url}" /patches/')
            for res in PATCHES[coord]:
                lines.append(f"RUN jar xf {jar} {res} && sed -i '/^#/d' {res} && jar uf {jar} {res}")
    lines += ["", "FROM ${BASE}", ""]
    for coord in r.lowered.manifest["artifacts"]:
        coord = coord.format(flink_minor=flink_minor)
        group, artifact = coord.split(":")[:2]
        arg = re.sub(r"[^A-Z0-9]", "_", artifact.upper()) + "_VERSION"
        pinned = coord.split(":")[2] if coord.count(":") == 2 else None  # custom connectors pin versions
        coord = f"{group}:{artifact}"
        default = pinned or versions.get(coord) or TESTED_VERSIONS.get(coord)
        lines.append(f"# {coord}" + ("" if default else f" (pass --build-arg {arg}=<version>)"))
        lines.append(f"ARG {arg}" + (f"={default}" if default else ""))
        if coord in patched:
            lines.append(f"COPY --from=cascade-patches /patches/{artifact}-${{{arg}}}.jar /opt/flink/lib/")
            continue
        url = (f"https://repo1.maven.org/maven2/{group.replace('.', '/')}/{artifact}/${{{arg}}}/"
               f"{artifact}-${{{arg}}}.jar")
        lines.append(f'RUN test -n "${{{arg}}}" && wget -q -P /opt/flink/lib "{url}"')
    p = r.checked.pipeline
    if needs_python(p):
        lines += ["", "# Python UDFs / process models: PyFlink in its own virtualenv, Python support on Flink's classpath",
                  "# (Flink images run as flink, uid 9999; installing packages needs root)",
                  "USER root",
                  "RUN apt-get update -y && apt-get install -y --no-install-recommends python3 python3-venv \\",
                  "    && rm -rf /var/lib/apt/lists/*",
                  f"RUN python3 -m venv /opt/pyflink && /opt/pyflink/bin/pip install --no-cache-dir "
                  f"apache-flink=={flink}",
                  "RUN cp /opt/flink/opt/flink-python-*.jar /opt/flink/lib/",
                  "USER 9999"]
    lines += ["", f"COPY runtime/target/cascade-runtime.jar {RUNTIME_JAR}"]
    project = os.path.relpath(p.root_dir)
    for f in p.functions.values():
        if f.language == "java" and f.jar:
            lines.append(f"COPY {os.path.join(project, f.jar)} {USRLIB}/{os.path.basename(f.jar)}")
    for m in p.models.values():
        if m.process and m.process.get("jar") and m.copy_of is None:
            jar = m.process["jar"]
            lines.append(f"COPY {os.path.join(project, jar)} {USRLIB}/{os.path.basename(jar)}")
    for jar in cfg.get("jars") or []:
        lines.append(f"COPY {jar} {USRLIB}/{os.path.basename(str(jar))}")
    return "\n".join(lines) + "\n"

