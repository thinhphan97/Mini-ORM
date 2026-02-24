#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! python3 -c "import build, setuptools, wheel" >/dev/null 2>&1; then
  echo "Missing build toolchain packages (build/setuptools/wheel). Install with:"
  echo "  pip install -r requirements-build.txt"
  exit 1
fi

rm -rf dist build ./*.egg-info
# Use current environment instead of isolated PEP 517 env:
# faster/offline-friendly, but depends on pinned local build toolchain.
python3 -m build --sdist --wheel --no-isolation

echo "Build complete. Artifacts are in ./dist"
