#!/usr/bin/env bash
# Build the cascade-dsl JVM artifacts against a Flink distribution:
#   target/cascade-runtime.jar  UDFs referenced by generated SQL (ships with every job)
#   target/cascade-tools.jar    local tooling: exact schemas, test runner, compiled plans, restore tests
# Usage: FLINK_HOME=/path/to/flink runtime/build.sh
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
: "${FLINK_HOME:?set FLINK_HOME to a Flink distribution}"
rm -rf "$here/target" && mkdir -p "$here/target/runtime" "$here/target/tools"
javac --release 11 -nowarn -cp "$FLINK_HOME/lib/*" -d "$here/target/runtime" \
  $(find "$here/src" -name '*.java')
jar cf "$here/target/cascade-runtime.jar" -C "$here/target/runtime" .
javac --release 11 -nowarn -cp "$FLINK_HOME/lib/*:$here/target/runtime" -d "$here/target/tools" \
  $(find "$here/tools" -name '*.java')
cp -r "$here/tools/META-INF" "$here/target/tools/"
jar cf "$here/target/cascade-tools.jar" -C "$here/target/tools" .
echo "built $here/target/cascade-runtime.jar and $here/target/cascade-tools.jar"
