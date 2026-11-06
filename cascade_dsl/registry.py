"""Import source columns from a Confluent-compatible schema registry (or an .avsc file).

    cascade import-schema --registry http://registry:8081 --subject orders.v1-value --source orders

prints a `columns:` block (Avro -> Flink types) ready to paste into pipeline.yaml. This is an
explicit, reviewable step rather than a compile-time lookup, so compilation never depends on the
network and schema changes show up as diffs.

Basic auth: set CASCADE_REGISTRY_USER_INFO=user:password.
"""
from __future__ import annotations

import base64
import json
import os
import urllib.parse
import urllib.request

PRIMITIVES = {"boolean": "BOOLEAN", "int": "INT", "long": "BIGINT", "float": "FLOAT", "double": "DOUBLE",
              "bytes": "BYTES", "string": "STRING"}
LOGICAL = {
    ("int", "date"): "DATE",
    ("int", "time-millis"): "TIME(3)",
    ("long", "time-micros"): "TIME(6)",
    ("long", "timestamp-millis"): "TIMESTAMP_LTZ(3)",
    ("long", "timestamp-micros"): "TIMESTAMP_LTZ(6)",
    ("long", "local-timestamp-millis"): "TIMESTAMP(3)",
    ("long", "local-timestamp-micros"): "TIMESTAMP(6)",
    ("string", "uuid"): "STRING",
}


class SchemaImportError(ValueError):
    pass


def fetch(registry_url: str, subject: str, version: str = "latest", timeout: int = 15) -> tuple[str, str]:
    """Returns (schema type, schema text) for a subject version."""
    url = (f"{registry_url.rstrip('/')}/subjects/{urllib.parse.quote(subject, safe='')}"
           f"/versions/{urllib.parse.quote(str(version), safe='')}")
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.schemaregistry.v1+json"})
    user_info = os.environ.get("CASCADE_REGISTRY_USER_INFO")
    if user_info:
        req.add_header("Authorization", "Basic " + base64.b64encode(user_info.encode()).decode())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.load(resp)
    except urllib.error.HTTPError as e:
        raise SchemaImportError(f"registry returned HTTP {e.code} for {url}") from e
    except (urllib.error.URLError, OSError) as e:
        raise SchemaImportError(f"cannot reach registry at {registry_url}: {e}") from e
    return body.get("schemaType", "AVRO"), body["schema"]


def avro_columns(schema) -> list[tuple[str, str]]:
    """Top-level record fields of an Avro schema as (column, Flink type)."""
    if isinstance(schema, str):
        try:
            schema = json.loads(schema)
        except ValueError as e:
            raise SchemaImportError(f"not a JSON Avro schema: {e}") from e
    if not (isinstance(schema, dict) and schema.get("type") == "record"):
        raise SchemaImportError("the top-level Avro schema must be a record")
    named: dict[str, str] = {}
    return [(f["name"], _field_type(f["type"], named, f["name"])) for f in schema["fields"]]


def _field_type(t, named: dict, path: str) -> str:
    nullable = False
    if isinstance(t, list):
        branches = [b for b in t if b != "null"]
        nullable = len(branches) != len(t)
        if len(branches) != 1:
            raise SchemaImportError(f"field '{path}': unions of several non-null types are not supported "
                                    f"({t}); model them as separate columns")
        t = branches[0]
    flink = _type(t, named, path)
    return flink if nullable else f"{flink} NOT NULL"


def _type(t, named: dict, path: str) -> str:
    if isinstance(t, str):
        if t in PRIMITIVES:
            return PRIMITIVES[t]
        if t in named:
            return named[t]
        raise SchemaImportError(f"field '{path}': unknown Avro type '{t}'")
    if isinstance(t, list):
        return _field_type(t, named, path).removesuffix(" NOT NULL")
    kind = t.get("type")
    logical = t.get("logicalType")
    if logical == "decimal" and kind in ("bytes", "fixed"):
        out = f"DECIMAL({t['precision']}, {t.get('scale', 0)})"
    elif (kind, logical) in LOGICAL:
        out = LOGICAL[(kind, logical)]
    elif kind in PRIMITIVES:
        out = PRIMITIVES[kind]
    elif kind == "enum":
        out = "STRING"
    elif kind == "fixed":
        out = f"BINARY({t['size']})"
    elif kind == "array":
        out = f"ARRAY<{_field_type(t['items'], named, path + '[]')}>"
    elif kind == "map":
        out = f"MAP<STRING NOT NULL, {_field_type(t['values'], named, path + '{}')}>"
    elif kind == "record":
        fields = ", ".join(f"`{f['name']}` {_field_type(f['type'], named, path + '.' + f['name'])}"
                           for f in t["fields"])
        out = f"ROW<{fields}>"
    else:
        raise SchemaImportError(f"field '{path}': unsupported Avro type {json.dumps(t)}")
    if t.get("name") and kind in ("record", "enum", "fixed"):
        named[t["name"]] = out
        if t.get("namespace"):
            named[f"{t['namespace']}.{t['name']}"] = out
    return out


def columns_yaml(columns: list[tuple[str, str]], source: str | None = None, indent: int = 4) -> str:
    pad = " " * indent
    lines = [f"{pad}columns:"] + [f"{pad}  {name}: {typ}" for name, typ in columns]
    if source:
        lines = [f"  {source}:"] + lines
    return "\n".join(lines) + "\n"
