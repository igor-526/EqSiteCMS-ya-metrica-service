#!/bin/sh
set -eu

fixture='value={  "unformatted":True}'
cleanup() {
    rm -rf .github/format-regression .helm/format-regression
    rmdir .github .helm 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM
mkdir -p .github/format-regression .helm/format-regression
printf '%s\n' "$fixture" > .github/format-regression/fixture.py
printf '%s\n' "$fixture" > .helm/format-regression/fixture.py
uv run ruff check . --fix >/dev/null
uv run ruff format . >/dev/null
test "$(cat .github/format-regression/fixture.py)" = "$fixture"
test "$(cat .helm/format-regression/fixture.py)" = "$fixture"
