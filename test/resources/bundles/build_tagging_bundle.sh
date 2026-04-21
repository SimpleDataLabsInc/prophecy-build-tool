#!/usr/bin/env bash
#
# Regenerate test/resources/tagging.bundle — a tiny git repo shaped like the
# prophecy-samples/HelloProphecy layout used by the legacy tagging tests.
#
# Layout:
#   main branch
#   ├── prophecy/              (python project, version 0.0.1)
#   │   └── pbt_project.yml
#   └── prophecy_scala/        (scala project, version 0.0.1)
#       └── pbt_project.yml
#
# The versions are intentionally ``0.0.1`` in both so branch-based tag names
# like ``<branch>/0.0.1`` are deterministic.
#
# Usage:
#   ./build_tagging_bundle.sh
#   # produces test/resources/tagging.bundle

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
out="$here/../tagging.bundle"
work="$here/.tmp-tagging-$$"
rm -rf "$work"
mkdir -p "$work"
trap "rm -rf '$work'" EXIT

cd "$work"
git init -q -b main --template=
git config user.email "pbt-test@example.com"
git config user.name  "PBT Test"
git config commit.gpgsign false
git config core.hooksPath /dev/null

mkdir -p prophecy prophecy_scala
cat >prophecy/pbt_project.yml <<EOF
name: tagging-fixture-py
language: python
version: 0.0.1
pipelines: {}
jobs: {}
EOF
cat >prophecy_scala/pbt_project.yml <<EOF
name: tagging-fixture-scala
language: scala
version: 0.0.1
pipelines: {}
jobs: {}
EOF

git add -A
git commit -q -m "initial tagging fixture"

git bundle create "$out" --all >/dev/null
echo "Wrote $out"
