#!/usr/bin/env bash
# Build udfs/acme-functions.jar from udfs/java. -parameters keeps parameter names, which Flink needs to
# name a process table function's state. Usage: FLINK_HOME=/path/to/flink udfs/build.sh
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
: "${FLINK_HOME:?set FLINK_HOME to a Flink distribution}"
out="$(mktemp -d)"
javac --release 11 -parameters -nowarn -cp "$FLINK_HOME/lib/*" -d "$out" $(find "$here/java" -name '*.java')
jar cf "$here/acme-functions.jar" -C "$out" .
rm -rf "$out"
echo "built $here/acme-functions.jar"
