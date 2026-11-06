#!/usr/bin/env bash
# Build the cascade-dsl runtime UDF jar against a Flink distribution.
# Usage: FLINK_HOME=/path/to/flink runtime/build.sh
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
: "${FLINK_HOME:?set FLINK_HOME to a Flink distribution}"
rm -rf "$here/target" && mkdir -p "$here/target/classes"
javac --release 11 -cp "$FLINK_HOME/lib/*" -d "$here/target/classes" \
  $(find "$here/src" -name '*.java')
jar cf "$here/target/cascade-runtime.jar" -C "$here/target/classes" .
echo "built $here/target/cascade-runtime.jar"
