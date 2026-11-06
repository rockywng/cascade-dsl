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
from .jvm import script
from .lower import Stmt
from .udfs import python_files, python_functions

RUNNER = "dev.cascade.runner.SqlRunner"
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
    image = image or cfg.get("image")
    if not image:
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
    setup = [_in_image(st) for st in lowered.statements if st.kind != "insert"]
    files = {"setup.sql": _header(r, "setup") + script(setup)}
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

    labels = {"app.kubernetes.io/name": name, "app.kubernetes.io/managed-by": "cascade",
              "cascade.dev/pipeline": p.name, **({"cascade.dev/environment": p.env} if p.env else {}),
              **{str(k): str(v) for k, v in (cfg.get("labels") or {}).items()}}
    namespace = cfg.get("namespace")
    meta = lambda n: {"name": n, **({"namespace": namespace} if namespace else {}), "labels": labels}  # noqa: E731

    configmap = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta(f"{name}-job"), "data": files}
    deployment = _flink_deployment(r, name, meta(name), image, args, secret_name, cfg)
    resources = ["configmap.yaml", "flinkdeployment.yaml"]
    if python_functions(p):
        py_cm = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta(f"{name}-python"),
                 "data": python_files(p)}
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
    if secrets:
        write("secret.template.yaml", "# Fill in and apply separately (or create it from your secret manager).\n"
              "# Never commit real values.\n" + _dump(secret))
    write("kustomization.yaml", _dump(kustomization))
    write("Dockerfile", dockerfile(r, cfg))
    return {"name": name, "mode": mode, "secrets": secrets, "secret_name": secret_name, "out": out}


def _in_image(st: Stmt) -> Stmt:
    """Rewrite local paths in setup statements to where the image / pod has the files."""
    if st.kind == "function" and st.options.get("jar"):
        jar = st.options["jar"]
        target = f"{USRLIB}/{os.path.basename(jar)}"
        return Stmt(st.kind, st.name, st.sql.replace(_sql_lit(jar), _sql_lit(target)), st.comment,
                    options={"jar": target})
    if st.kind == "set" and st.name == "python.files":
        return Stmt("set", st.name, f"SET 'python.files' = '{PYTHON_DIR}';", options={st.name: PYTHON_DIR})
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


def _flink_deployment(r: Result, name: str, metadata: dict, image: str, args: list[str], secret_name: str,
                      cfg: dict) -> dict:
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
    if python_functions(p):
        conf["python.executable"] = PYFLINK_PYTHON
        conf["python.client.executable"] = PYFLINK_PYTHON
    conf.update({str(k): str(v) for k, v in (cfg.get("flink_config") or {}).items()})

    jm = cfg.get("jobmanager") or {}
    env = [{"name": env_name(s), "valueFrom": {"secretKeyRef": {"name": secret_name, "key": secret_key(s)}}}
           for s in sorted(p.secrets)]
    container = {"name": "flink-main-container",
                 "volumeMounts": [{"name": "cascade-job", "mountPath": JOB_DIR, "readOnly": True}]}
    volumes = [{"name": "cascade-job", "configMap": {"name": f"{name}-job"}}]
    if python_functions(p):
        container["volumeMounts"].append({"name": "cascade-python", "mountPath": PYTHON_DIR, "readOnly": True})
        volumes.append({"name": "cascade-python", "configMap": {"name": f"{name}-python"}})
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
            "podTemplate": {"spec": {
                "containers": [container],
                "volumes": volumes,
            }},
            "job": {
                "jarURI": f"local://{RUNTIME_JAR}",
                "entryClass": RUNNER,
                "args": args,
                "parallelism": (sc.parallelism if sc and sc.parallelism else 1),
                "upgradeMode": cfg.get("upgrade_mode", "savepoint"),
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
             f"FROM flink:{flink}-scala_2.12-java17", ""]
    for coord in r.lowered.manifest["artifacts"]:
        group, artifact = coord.split(":")[:2]
        arg = re.sub(r"[^A-Z0-9]", "_", artifact.upper()) + "_VERSION"
        default = versions.get(coord)
        lines.append(f"# {coord}" + ("" if default else f" (pass --build-arg {arg}=<version>)"))
        lines.append(f"ARG {arg}" + (f"={default}" if default else ""))
        url = (f"https://repo1.maven.org/maven2/{group.replace('.', '/')}/{artifact}/${{{arg}}}/"
               f"{artifact}-${{{arg}}}.jar")
        lines.append(f'RUN test -n "${{{arg}}}" && wget -q -P /opt/flink/lib "{url}"')
    p = r.checked.pipeline
    if python_functions(p):
        lines += ["", "# Python UDFs: PyFlink in its own virtualenv, Python support on Flink's classpath",
                  "RUN apt-get update -y && apt-get install -y --no-install-recommends python3 python3-venv \\",
                  "    && rm -rf /var/lib/apt/lists/*",
                  f"RUN python3 -m venv /opt/pyflink && /opt/pyflink/bin/pip install --no-cache-dir "
                  f"apache-flink=={flink}",
                  "RUN cp /opt/flink/opt/flink-python-*.jar /opt/flink/lib/"]
    lines += ["", f"COPY runtime/target/cascade-runtime.jar {RUNTIME_JAR}"]
    project = os.path.relpath(p.root_dir)
    for f in p.functions.values():
        if f.language == "java" and f.jar:
            lines.append(f"COPY {os.path.join(project, f.jar)} {USRLIB}/{os.path.basename(f.jar)}")
    for jar in cfg.get("jars") or []:
        lines.append(f"COPY {jar} {USRLIB}/{os.path.basename(str(jar))}")
    return "\n".join(lines) + "\n"

