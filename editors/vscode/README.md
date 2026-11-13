# cascade for VS Code

Diagnostics, completion and hover for `pipeline.yaml`, from `cascade lsp` (the compiler itself, so
editors see exactly what `cascade validate` reports).

    cd editors/vscode && npm install && npx @vscode/vsce package   # then: Install from VSIX

Set `cascade.command` if `cascade` isn't on PATH, e.g. `["python3", "-m", "cascade_dsl", "lsp"]`.
Other editors: any LSP client can run `cascade lsp` over stdio for files named `pipeline.yaml`.
