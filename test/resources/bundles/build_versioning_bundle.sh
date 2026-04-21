#!/usr/bin/env bash
#
# Regenerate test/resources/versioning.bundle, which ships a tiny git repo
# used by test/v2/test_versioning_git_e2e.py.
#
# Shape:
#   main                                 → pbt_project.yml version 0.0.1
#   pytest/test_big_version              → version 9999.0.0 (larger)
#   pytest/test_small_version            → version 0.0.0 (smaller)
#   pytest/test_bad_version              → version not-a-version (unparseable)
#   pytest/static_branch_name            → version 0.0.1 (for --make-unique)
#
# Also contains a pipelines/demo/code/setup.py whose version matches main.
#
# Usage:
#   ./build_versioning_bundle.sh
#   # produces test/resources/versioning.bundle

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
out="$here/../versioning.bundle"
# Use an in-workspace scratch dir; /tmp on some sandboxes blocks hook writes.
work="$here/.tmp-versioning-$$"
rm -rf "$work"
mkdir -p "$work"
trap "rm -rf '$work'" EXIT

cd "$work"
# ``--template=`` avoids copying /etc/gitconfig hooks which can be blocked in
# sandboxed environments.
git init -q -b main --template=
git config user.email "pbt-test@example.com"
git config user.name  "PBT Test"
git config commit.gpgsign false
git config core.hooksPath /dev/null

write_project() {
  local version="$1"
  mkdir -p pipelines/demo/code
  cat >pbt_project.yml <<EOF
name: versioning-fixture
language: python
version: $version
pipelines:
  pipelines/demo:
    name: demo
    language: python
jobs: {}
EOF
  cat >pipelines/demo/code/setup.py <<EOF
from setuptools import setup
setup(
    name='demo',
    version='$version'
)
EOF
}

write_project "0.0.1"
git add -A
git commit -q -m "initial: 0.0.1"

for branch_version in \
  "pytest/test_big_version:9999.0.0" \
  "pytest/test_small_version:0.0.0" \
  "pytest/test_bad_version:not-a-version"; do
  branch="${branch_version%%:*}"
  version="${branch_version##*:}"
  git checkout -q -b "$branch" main
  write_project "$version"
  git add -A
  git commit -q -m "branch $branch at $version"
  git checkout -q main
done

# ``pytest/static_branch_name`` is used by --make-unique tests. It just needs
# to exist (same content as main) so the tool can read the branch name, hash
# it, and stamp a deterministic sha-based suffix on the version.
git branch pytest/static_branch_name main

# Bundle all refs for offline clone.
git bundle create "$out" --all >/dev/null
echo "Wrote $out"
