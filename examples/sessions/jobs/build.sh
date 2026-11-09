#!/usr/bin/env bash
# Build jobs/acme-process.jar from jobs/java against Flink and the cascade runtime (the Process API).
# Usage: FLINK_HOME=/path/to/flink jobs/build.sh   (after runtime/build.sh)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
: "${FLINK_HOME:?set FLINK_HOME to a Flink distribution}"
runtime="${CASCADE_RUNTIME_JAR:-$here/../../../runtime/target/cascade-runtime.jar}"
out="$(mktemp -d)"
javac --release 11 -nowarn -cp "$FLINK_HOME/lib/*:$runtime" -d "$out" $(find "$here/java" -name '*.java')
jar cf "$here/acme-process.jar" -C "$out" .
rm -rf "$out"
echo "built $here/acme-process.jar"
