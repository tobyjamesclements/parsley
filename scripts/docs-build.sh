#!/usr/bin/env bash
#
# Build the docs site the way .github/workflows/pages.yml builds it, so a broken site
# surfaces here rather than after a push. MkDocs needs two generated inputs that are not in
# the tree — the Javadoc staged into docs/api, and llms-full.txt concatenated from llms.txt —
# so running mkdocs alone tests something the workflow never builds. This runs all three
# steps, and builds strictly, which turns any MkDocs warning into a failure.
#
# The toolchain is not a build dependency of the library and is not pinned here for the same
# reason the workflow does not pin it: both take whatever `pip install mkdocs-material mike`
# resolves. It lives in an untracked .venv, created on first run.
#
#   scripts/docs-build.sh              build to site/
#   scripts/docs-build.sh --serve      serve on http://127.0.0.1:8000, reloading on edit
#   scripts/docs-build.sh --skip-api   skip the Javadoc build, leaving docs/api as it is
#
# Deploying is a separate act, and only the workflow does it: mike owns the version aliases
# on the gh-pages branch, and a local deploy would push a version into that selector.
set -euo pipefail

cd "$(cd "$(dirname "$0")/.." && pwd)"

serve=false
skip_api=false
for arg in "$@"; do
    case "$arg" in
        --serve)    serve=true ;;
        --skip-api) skip_api=true ;;
        *) echo "usage: $0 [--serve] [--skip-api]" >&2; exit 2 ;;
    esac
done

if [ ! -x .venv/bin/mkdocs ]; then
    echo "==> Creating .venv with the MkDocs toolchain"
    python3 -m venv .venv
    .venv/bin/pip install --quiet --upgrade pip
    .venv/bin/pip install --quiet mkdocs-material mike
fi

if [ "$skip_api" = false ]; then
    echo "==> Building Javadoc into docs/api"
    ./mvnw javadoc:javadoc --no-transfer-progress -DskipTests -q
    rm -rf docs/api
    mkdir -p docs/api
    cp -r target/reports/apidocs/. docs/api/
fi

echo "==> Generating docs/llms-full.txt"
python3 scripts/build-llms-full.py .

if [ "$serve" = true ]; then
    exec .venv/bin/mkdocs serve --strict
fi

echo "==> Building site/"
.venv/bin/mkdocs build --strict
