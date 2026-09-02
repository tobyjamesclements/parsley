#!/usr/bin/env bash
#
# Build the docs site the way .github/workflows/pages.yml builds it, so a broken site
# surfaces here rather than after a push. MkDocs needs three generated inputs that are not in
# the tree — the Javadoc staged into docs/javadoc, llms-full.txt concatenated from llms.txt,
# and decisions.md indexed from DECISIONS.md — so running mkdocs alone tests something the
# workflow never builds. This runs every step, and builds strictly, which turns any MkDocs
# warning into a failure.
#
# The toolchain is not a build dependency of the library and is not pinned here for the same
# reason the workflow does not pin it: both take whatever `pip install mkdocs-material mike`
# resolves. It lives in an untracked .venv, created on first run.
#
#   scripts/docs-build.sh              build to site/
#   scripts/docs-build.sh --serve      serve on http://127.0.0.1:8000, reloading on edit
#   scripts/docs-build.sh --skip-api   skip the Javadoc build, leaving docs/javadoc as it is
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
    echo "==> Building Javadoc into docs/javadoc"
    ./mvnw javadoc:javadoc --no-transfer-progress -DskipTests -q
    rm -rf docs/javadoc
    mkdir -p docs/javadoc
    cp -r target/reports/apidocs/. docs/javadoc/
fi

echo "==> Generating docs/llms-full.txt"
python3 scripts/build-llms-full.py .

echo "==> Generating docs/decisions.md"
PARSLEY_DOCS_REF="$(git rev-parse HEAD)" python3 scripts/build-decisions-index.py .

if [ "$serve" = true ]; then
    exec .venv/bin/mkdocs serve --strict
fi

echo "==> Building site/"
.venv/bin/mkdocs build --strict

if [ "$skip_api" = false ]; then
    # The Javadoc's own index.html is the one staged file a docs page can silently displace:
    # a page whose name matches the staging directory renders to the same destination under
    # use_directory_urls, and MkDocs reports no conflict when the page wins. Strict mode does
    # not see it, and the only symptom is a "Browse the Javadoc" button that reloads its own
    # page, so check the built file is the generated one.
    echo "==> Checking the Javadoc index survived the build"
    if ! grep -q 'name="generator" content="javadoc' site/javadoc/index.html; then
        echo "site/javadoc/index.html is not the generated Javadoc index; a docs page" >&2
        echo "renders to that path and has displaced it." >&2
        exit 1
    fi
fi
