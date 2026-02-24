#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! python -c "import build" >/dev/null 2>&1; then
  echo "Missing Python package 'build'. Install with:"
  echo "  pip install -r requirements-build.txt"
  exit 1
fi

rm -rf dist build ./*.egg-info
python -m build --sdist --wheel --no-isolation

echo "Build complete. Artifacts are in ./dist"
