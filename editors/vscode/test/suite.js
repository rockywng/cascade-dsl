// Runs inside VS Code: open pipeline.yaml, wait for cascade's diagnostics, check go-to-definition.
const assert = require("assert");
const path = require("path");
const vscode = require("vscode");

async function waitFor(fn, ms) {
  const end = Date.now() + ms;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > end) return v;
    await new Promise((r) => setTimeout(r, 500));
  }
}

async function run() {
  const file = path.join(vscode.workspace.workspaceFolders[0].uri.fsPath, "pipeline.yaml");
  const doc = await vscode.workspace.openTextDocument(file);
  await vscode.window.showTextDocument(doc);
  const ext = vscode.extensions.all.find((e) => e.packageJSON.name === "cascade-dsl");
  assert.ok(ext, "the extension is loaded");
  await ext.activate();

  const diags = await waitFor(() => {
    const d = vscode.languages.getDiagnostics(doc.uri).filter((x) => x.source === "cascade");
    return d.length ? d : null;
  }, 60000);
  assert.ok(diags, "cascade published diagnostics");
  const bad = diags.find((d) => String(d.code) === "CD302");
  assert.ok(bad, "CD302 for the unknown table: " + JSON.stringify(diags.map((d) => d.message)));
  assert.match(bad.message, /ordrs/);
  console.log("cascade diagnostics in VS Code: " + diags.map((d) => d.code).join(", "));

  // go to definition: `clean` in the sink's `from:` -> the model's declaration
  const line = doc.lineAt(doc.lineCount - 2).text.includes("from: clean") ? doc.lineCount - 2 : doc.lineCount - 1;
  const col = doc.lineAt(line).text.indexOf("clean") + 1;
  const defs = await vscode.commands.executeCommand("vscode.executeDefinitionProvider", doc.uri,
    new vscode.Position(line, col));
  assert.ok(defs && defs.length, "a definition for `clean`");
  const target = defs[0].range ? defs[0].range.start.line : defs[0].targetRange.start.line;
  assert.ok(doc.lineAt(target).text.startsWith("  clean:"), "jumps to the model: " + doc.lineAt(target).text);
  console.log("go to definition: line " + (target + 1));
}

module.exports = { run };
