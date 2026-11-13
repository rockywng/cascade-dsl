"""`cascade lsp`: a Language Server (LSP over stdio) for pipeline.yaml.

- diagnostics: the compiler's own (every CD code), as you type -- unsaved buffers are compiled from a
  temporary copy of the project, so other files (SQL, Python) are read as they are on disk
- completion: connector types after `type:`, the connector's properties inside a connection, source or
  sink, and the top-level / source / model / sink keys
- hover: the meaning of a diagnostic code (CD...) under the cursor
- go to definition / find references: a source, model, sink, connection or function name -- in SQL,
  `from:`, `connection:`, `inputs:` -- to where it's declared, and every use of it

Editors: VS Code via editors/vscode (it starts `cascade lsp`); any LSP client otherwise.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
import urllib.parse

from .compiler import compile_pipeline
from .connectors import CATALOG
from .diagnostics import Severity

SEVERITY = {Severity.ERROR: 1, Severity.WARNING: 2, Severity.INFO: 3}


def uri_to_path(uri: str) -> str:
    return urllib.parse.unquote(urllib.parse.urlparse(uri).path)


def path_to_uri(path: str) -> str:
    return "file://" + urllib.parse.quote(os.path.abspath(path))


# --- diagnostics -----------------------------------------------------------------------------------

def diagnose(path: str, text: str | None = None) -> list[dict]:
    """LSP diagnostics for pipeline.yaml at `path`, compiled with `text` as its content if given."""
    project = os.path.dirname(os.path.abspath(path))
    work = None
    target = project
    if text is not None:
        work = tempfile.mkdtemp(prefix="cascade-lsp-")
        target = os.path.join(work, "project")
        shutil.copytree(project, target, ignore=shutil.ignore_patterns(".git", "target", ".cascade", "deploy",
                                                                       "plan", "__pycache__"))
        with open(os.path.join(target, os.path.basename(path)), "w") as f:
            f.write(text)
    cwd = os.getcwd()
    try:
        os.chdir(target)  # diagnostics' paths are relative to the working directory
        r = compile_pipeline(".", emit=False)
    finally:
        os.chdir(cwd)
        if work:
            shutil.rmtree(work, ignore_errors=True)
    out = []
    for d in r.diagnostics.items:
        loc = d.location
        if loc is not None and os.path.basename(loc.file) != os.path.basename(path):
            continue  # reported on another file (an SQL file, a Python module)
        line = max((loc.line or 1) - 1, 0) if loc else 0
        msg = d.message + (f"\nhint: {d.hint}" if d.hint else "")
        out.append({"range": {"start": {"line": line, "character": 0}, "end": {"line": line, "character": 1000}},
                    "severity": SEVERITY.get(d.severity, 3), "code": d.code, "source": "cascade", "message": msg})
    return out


# --- completion -------------------------------------------------------------------------------------

TOP = ["pipeline", "description", "owner", "mode", "defaults", "flink", "vars", "environments", "connections",
       "sources", "models", "sinks", "quarantine", "scaling", "deploy", "functions"]
SOURCE = ["connection", "columns", "primary_key", "event_time", "processing_time", "options", "description",
          "format", "from_pipeline", "sink"]
MODEL = ["sql", "sql_file", "process", "columns", "expectations", "description", "allow_unbounded_state",
         "scaling", "backfill"]
SINK = ["from", "connection", "mode", "primary_key", "options", "description", "format", "on_conflict"]


def _section(lines: list[str], row: int) -> tuple[str | None, int]:
    """The top-level section the cursor is in, and the indent of the cursor's line."""
    for i in range(row, -1, -1):
        m = re.match(r"^([A-Za-z_]+):", lines[i])
        if m:
            return m.group(1), len(lines[row]) - len(lines[row].lstrip())
    return None, 0


def _connection_type(lines: list[str], row: int) -> str | None:
    """The connector type the cursor's block uses: its own `type:`, or that of its `connection:`."""
    text = "\n".join(lines)
    for i in range(row, -1, -1):
        m = re.search(r"\btype:\s*([a-z0-9-]+)", lines[i])
        if m:
            return m.group(1)
        m = re.search(r"\bconnection:\s*([A-Za-z0-9_]+)", lines[i])
        if m:
            ref = re.search(rf"^\s+{re.escape(m.group(1))}:.*?type:\s*([a-z0-9-]+)", text, re.M | re.S)
            return ref.group(1) if ref else m.group(1)
        if re.match(r"^\s{0,2}[A-Za-z_]+:\s*$", lines[i]) and i != row:
            break
    return None


def complete(text: str, line: int, character: int) -> list[dict]:
    lines = text.split("\n")
    if line >= len(lines):
        return []
    before = lines[line][:character]
    if re.search(r"\btype:\s*[a-z0-9-]*$", before):
        return [{"label": t, "kind": 12, "detail": CATALOG[t].doc[:80]} for t in sorted(CATALOG)]
    section, indent = _section(lines, line)
    if indent == 0:
        return [{"label": k, "kind": 10} for k in TOP]
    keys = {"sources": SOURCE, "models": MODEL, "sinks": SINK}.get(section, [])
    spec = CATALOG.get(_connection_type(lines, line) or "")
    props = []
    if spec is not None:
        props = list(spec.connection if section == "connections" else
                     spec.source if section == "sources" else spec.sink if section == "sinks" else [])
    return ([{"label": k, "kind": 10, "detail": "property of " + spec.type} for k in props] if spec else []) + \
        [{"label": k, "kind": 10} for k in keys if k not in props]


# --- hover --------------------------------------------------------------------------------------------

def hover(text: str, line: int, character: int) -> dict | None:
    lines = text.split("\n")
    if line >= len(lines):
        return None
    for m in re.finditer(r"CD\d{3}", lines[line]):
        if m.start() <= character <= m.end():
            return {"contents": {"kind": "markdown", "value": f"**{m.group(0)}**: {code_doc(m.group(0))}"}}
    return None


def code_doc(code: str) -> str:
    """The diagnostic-code table row of docs/DSL.md that covers `code`."""
    doc = os.path.join(os.path.dirname(__file__), "..", "docs", "DSL.md")
    try:
        with open(doc) as f:
            rows = [x for x in f if x.startswith("| CD")]
    except OSError:
        return "see docs/DSL.md, Diagnostic codes"
    n = int(code[2:])
    for row in rows:  # a row naming the code, else the row whose range covers it
        if code in row:
            return row[2:].partition("|")[2].strip(" |\n")
    for row in rows:
        cell, _, rest = row[2:].partition("|")
        if any(lo <= n <= hi for lo, hi in _ranges(cell)):
            return rest.strip(" |\n")
    return "see docs/DSL.md, Diagnostic codes"


def _ranges(cell: str) -> list[tuple[int, int]]:
    out = []
    for a, b in re.findall(r"CD(\d+x+)(?:–CD(\d+x+))?", cell):
        lo = int(a.replace("x", "0"))
        hi = int((b or a).replace("x", "9"))
        out.append((lo, hi))
    return out


# --- definitions and references ------------------------------------------------------------------------

SECTIONS = ("connections", "sources", "models", "sinks", "functions")


def definitions(text: str) -> dict[str, tuple[int, int]]:
    """Name -> (line, column) of every connection, source, model, sink and function declared."""
    out, section = {}, None
    for i, line in enumerate(text.split("\n")):
        top = re.match(r"^([A-Za-z_]+):", line)
        if top:
            section = top.group(1)
            continue
        m = re.match(r"^  ([A-Za-z_][A-Za-z0-9_]*):", line)
        if m and section in SECTIONS and m.group(1) not in out:
            out[m.group(1)] = (i, 2)
    return out


def _word_at(line: str, character: int) -> str | None:
    for m in re.finditer(r"[A-Za-z_][A-Za-z0-9_]*", line):
        if m.start() <= character <= m.end():
            return m.group(0)
    return None


def definition(text: str, line: int, character: int) -> tuple[int, int] | None:
    lines = text.split("\n")
    if line >= len(lines):
        return None
    word = _word_at(lines[line], character)
    return definitions(text).get(word) if word else None


def references(text: str, line: int, character: int) -> list[tuple[int, int, int]]:
    """Every whole-word use of the declared name under the cursor: (line, start, end)."""
    lines = text.split("\n")
    if line >= len(lines):
        return []
    word = _word_at(lines[line], character)
    if not word or word not in definitions(text):
        return []
    pat = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(word)}(?![A-Za-z0-9_])")
    return [(i, m.start(), m.end()) for i, l in enumerate(lines) for m in pat.finditer(l)]


# --- the server -----------------------------------------------------------------------------------------

class Server:
    def __init__(self, inp=None, out=None):
        self.inp = inp or sys.stdin.buffer
        self.out = out or sys.stdout.buffer
        self.docs: dict[str, str] = {}

    def read(self) -> dict | None:
        length = 0
        while True:
            header = self.inp.readline()
            if not header:
                return None
            header = header.decode().strip()
            if not header:
                break
            if header.lower().startswith("content-length:"):
                length = int(header.split(":")[1])
        return json.loads(self.inp.read(length))

    def send(self, msg: dict):
        body = json.dumps(msg).encode()
        self.out.write(f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
        self.out.flush()

    def publish(self, uri: str):
        path = uri_to_path(uri)
        if os.path.basename(path) != "pipeline.yaml":
            return
        try:
            diags = diagnose(path, self.docs.get(uri))
        except Exception as e:  # noqa: BLE001 -- never take the editor down
            diags = [{"range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 1}},
                      "severity": 1, "source": "cascade", "message": f"cascade could not check this file: {e}"}]
        self.send({"jsonrpc": "2.0", "method": "textDocument/publishDiagnostics",
                   "params": {"uri": uri, "diagnostics": diags}})

    def handle(self, msg: dict) -> bool:
        method, params, mid = msg.get("method"), msg.get("params") or {}, msg.get("id")
        if method == "initialize":
            self.send({"jsonrpc": "2.0", "id": mid, "result": {
                "capabilities": {"textDocumentSync": {"openClose": True, "change": 1, "save": True},
                                 "completionProvider": {"triggerCharacters": [":", " "]},
                                 "hoverProvider": True, "definitionProvider": True, "referencesProvider": True},
                "serverInfo": {"name": "cascade"}}})
        elif method in ("textDocument/didOpen",):
            doc = params["textDocument"]
            self.docs[doc["uri"]] = doc["text"]
            self.publish(doc["uri"])
        elif method == "textDocument/didChange":
            uri = params["textDocument"]["uri"]
            self.docs[uri] = params["contentChanges"][-1]["text"]
            self.publish(uri)
        elif method == "textDocument/didSave":
            self.publish(params["textDocument"]["uri"])
        elif method == "textDocument/didClose":
            self.docs.pop(params["textDocument"]["uri"], None)
        elif method == "textDocument/completion":
            pos = params["position"]
            text = self.docs.get(params["textDocument"]["uri"], "")
            self.send({"jsonrpc": "2.0", "id": mid, "result": complete(text, pos["line"], pos["character"])})
        elif method == "textDocument/hover":
            pos = params["position"]
            text = self.docs.get(params["textDocument"]["uri"], "")
            self.send({"jsonrpc": "2.0", "id": mid, "result": hover(text, pos["line"], pos["character"])})
        elif method == "textDocument/definition":
            pos, uri = params["position"], params["textDocument"]["uri"]
            hit = definition(self.docs.get(uri, ""), pos["line"], pos["character"])
            self.send({"jsonrpc": "2.0", "id": mid, "result": None if hit is None else {
                "uri": uri, "range": {"start": {"line": hit[0], "character": hit[1]},
                                      "end": {"line": hit[0], "character": hit[1]}}}})
        elif method == "textDocument/references":
            pos, uri = params["position"], params["textDocument"]["uri"]
            refs = references(self.docs.get(uri, ""), pos["line"], pos["character"])
            self.send({"jsonrpc": "2.0", "id": mid, "result": [
                {"uri": uri, "range": {"start": {"line": a, "character": b}, "end": {"line": a, "character": c}}}
                for a, b, c in refs]})
        elif method == "shutdown":
            self.send({"jsonrpc": "2.0", "id": mid, "result": None})
        elif method == "exit":
            return False
        elif mid is not None:
            self.send({"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": f"unsupported: {method}"}})
        return True

    def serve(self):
        while True:
            msg = self.read()
            if msg is None or not self.handle(msg):
                return


def main() -> int:
    Server().serve()
    return 0
