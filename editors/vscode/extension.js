// Starts `cascade lsp` for pipeline.yaml files: the compiler's diagnostics as you type, completion of
// connector types and properties, and hover on diagnostic codes.
const vscode = require("vscode");
const { LanguageClient } = require("vscode-languageclient/node");

let client;

function activate(context) {
  const [command, ...args] = vscode.workspace.getConfiguration("cascade").get("command");
  client = new LanguageClient(
    "cascade",
    "cascade",
    { command, args },
    {
      documentSelector: [{ scheme: "file", language: "yaml", pattern: "**/pipeline.yaml" }],
      synchronize: { fileEvents: vscode.workspace.createFileSystemWatcher("**/*.{sql,py,yaml}") },
    },
  );
  context.subscriptions.push({ dispose: () => client && client.stop() });
  return client.start();
}

function deactivate() {
  return client ? client.stop() : undefined;
}

module.exports = { activate, deactivate };
