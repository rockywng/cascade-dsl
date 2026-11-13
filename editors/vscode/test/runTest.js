// End-to-end: a real VS Code (downloaded by @vscode/test-electron) with this extension, on a fixture
// workspace whose pipeline.yaml references an unknown table. Run under xvfb in CI.
const path = require("path");
const { runTests } = require("@vscode/test-electron");

async function main() {
  const extensionDevelopmentPath = path.resolve(__dirname, "..");
  const extensionTestsPath = path.resolve(__dirname, "suite.js");
  const workspace = path.resolve(__dirname, "fixture");
  await runTests({ extensionDevelopmentPath, extensionTestsPath, launchArgs: [workspace, "--disable-extensions"] });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
