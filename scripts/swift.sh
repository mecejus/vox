#!/bin/sh
# `swift` with the Command Line Tools workaround applied when needed.
# Usage: scripts/swift.sh build -c release | scripts/swift.sh test
set -e
cd "$(dirname "$0")/.."
. scripts/swiftpm-env.sh
# On a clean Command Line Tools build, `swift test` does not find the
# swift-testing macro plugin even though it is on the plugin path
# ("plugin for module 'TestingMacros' not found"). Loading it by name works.
_macros="$(dirname "$(xcrun --find swift)")/../lib/swift/host/plugins/testing/libTestingMacros.dylib"
if [ "$1" = "test" ] && [ -f "$_macros" ]; then
  exec swift "$@" -Xswiftc -load-plugin-library -Xswiftc "$_macros"
fi
exec swift "$@"
