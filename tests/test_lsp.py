"""cascade lsp: a JSON-RPC session over pipes, as an editor would hold it."""
import json
import os
import shutil
import subprocess
import sys
import unittest

from cascade_dsl.lsp import code_doc, complete

from helpers import project

REPO = os.path.join(os.path.dirname(__file__), "..")


class LspSession:
    def __init__(self):
        self.p = subprocess.Popen([sys.executable, "-m", "cascade_dsl", "lsp"], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, cwd=REPO, env={**os.environ, "PYTHONPATH": REPO})
        self.next_id = 0

    def send(self, method, params, request=True):
        msg = {"jsonrpc": "2.0", "method": method, "params": params}
        if request:
            self.next_id += 1
            msg["id"] = self.next_id
        body = json.dumps(msg).encode()
        self.p.stdin.write(f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
        self.p.stdin.flush()
        return msg.get("id")

    def recv(self):
        length = 0
        while True:
            line = self.p.stdout.readline().decode().strip()
            if not line:
                break
            if line.lower().startswith("content-length"):
                length = int(line.split(":")[1])
        return json.loads(self.p.stdout.read(length))

    def until(self, pred):
        while True:
            m = self.recv()
            if pred(m):
                return m

    def close(self):
        self.send("shutdown", None)
        self.send("exit", None, request=False)
        self.p.wait(10)


class LspTest(unittest.TestCase):
    def test_diagnostics_completion_and_hover(self):
        root = project("""
models:
  m: {sql: SELECT order_id FROM ordrs}
sinks:
  out: {from: m, connection: print}
""")
        s = LspSession()
        try:
            init = s.send("initialize", {"capabilities": {}})
            caps = s.until(lambda m: m.get("id") == init)["result"]["capabilities"]
            self.assertTrue(caps["hoverProvider"])
            uri = "file://" + os.path.join(root, "pipeline.yaml")
            with open(os.path.join(root, "pipeline.yaml")) as f:
                text = f.read()
            s.send("textDocument/didOpen", {"textDocument": {"uri": uri, "text": text, "languageId": "yaml",
                                                              "version": 1}}, request=False)
            diags = s.until(lambda m: m.get("method") == "textDocument/publishDiagnostics")["params"]["diagnostics"]
            bad = next(d for d in diags if d["code"] == "CD302")
            self.assertIn("ordrs", bad["message"])
            self.assertEqual(text.split("\n")[bad["range"]["start"]["line"]].strip()[:2], "m:")
            # an unsaved fix clears it
            s.send("textDocument/didChange", {"textDocument": {"uri": uri, "version": 2},
                                              "contentChanges": [{"text": text.replace("ordrs", "orders")}]},
                   request=False)
            diags = s.until(lambda m: m.get("method") == "textDocument/publishDiagnostics")["params"]["diagnostics"]
            self.assertFalse([d for d in diags if d["severity"] == 1], diags)
            cid = s.send("textDocument/completion", {"textDocument": {"uri": uri}, "position": {"line": 0,
                                                                                                "character": 0}})
            labels = {c["label"] for c in s.until(lambda m: m.get("id") == cid)["result"]}
            self.assertIn("sources", labels)
            s.close()
        finally:
            shutil.rmtree(root)

    def test_completion_of_connector_types_and_properties(self):
        text = "connections:\n  bus:\n    type: ka\n"
        self.assertIn("kafka", {c["label"] for c in complete(text, 2, len("    type: ka"))})
        text = "connections:\n  bus:\n    type: kafka\n    \n"
        self.assertIn("bootstrap_servers", {c["label"] for c in complete(text, 3, 4)})

    def test_definition_and_references(self):
        from cascade_dsl.lsp import definition, references
        text = ("sources:\n  orders:\n    connection: bus\nmodels:\n  clean:\n    sql: SELECT * FROM orders\n"
                "sinks:\n  out: {from: clean, connection: print}\n")
        lines = text.split("\n")
        self.assertEqual(definition(text, 5, lines[5].index("orders") + 2), (1, 2))
        self.assertEqual(definition(text, 7, lines[7].index("clean") + 1), (4, 2))
        self.assertEqual([r[0] for r in references(text, 1, 3)], [1, 5])
        self.assertIsNone(definition(text, 5, lines[5].index("SELECT")))

    def test_code_docs(self):
        self.assertIn("idle timeout", code_doc("CD108"))
        self.assertIn("sinks", code_doc("CD404"))


if __name__ == "__main__":
    unittest.main()
