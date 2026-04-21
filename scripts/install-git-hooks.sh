#!/usr/bin/env bash
# Point this repository at the tracked hooks in githooks/ (currently: pre-commit → black --check).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT}"

if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo "install-git-hooks: not a git repository (expected ${ROOT})" >&2
  exit 1
fi

HOOKS_DIR="${ROOT}/githooks"
if [ ! -d "${HOOKS_DIR}" ]; then
  echo "install-git-hooks: missing ${HOOKS_DIR}" >&2
  exit 1
fi

chmod +x "${HOOKS_DIR}/pre-commit" 2>/dev/null || true

git config core.hooksPath githooks

echo "core.hooksPath is now 'githooks' (under ${ROOT})."
echo "Hook pre-commit: Black --check on staged *.py / *.pyi (matches CI lint job)."
