#!/usr/bin/env python3
"""Generate docs/llms-full.txt: every document llms.txt links to, in one file.

docs/llms.txt is the single source of truth for which documents matter to an agent and in
what order. This script reads it, resolves each raw.githubusercontent.com link back to the
file in this tree, and concatenates them. Adding a page to llms.txt is therefore the only
step needed to have it appear in llms-full.txt too; the two cannot drift.

A link that does not resolve to a file in this tree is an error, not a warning, so a stale
or mistyped llms.txt link fails the docs build instead of shipping a dead link to agents.

Usage: scripts/build-llms-full.py [repo-root] [output-path]
"""

import re
import sys
from pathlib import Path

# - [Title](https://raw.githubusercontent.com/<owner>/<repo>/<ref>/<path>): description
RAW_LINK = re.compile(
    r"^- \[(?P<title>[^\]]+)\]\("
    r"(?P<url>https://raw\.githubusercontent\.com/[^/]+/[^/]+/[^/]+/(?P<path>[^)]+))"
    r"\)(?::\s*(?P<blurb>.*))?$"
)

SEPARATOR = "\n\n---\n\n"


def parse(llms_txt: str):
    """Split llms.txt into its preamble and its ordered, de-duplicated list of documents."""
    preamble, docs, seen = [], [], set()
    in_preamble = True
    for line in llms_txt.splitlines():
        if line.startswith("## "):
            in_preamble = False
        match = RAW_LINK.match(line)
        if match:
            path = match.group("path")
            if path not in seen:
                seen.add(path)
                docs.append(match.groupdict())
        elif in_preamble:
            preamble.append(line)
    return "\n".join(preamble).rstrip(), docs


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    out = Path(sys.argv[2] if len(sys.argv) > 2 else root / "docs/llms-full.txt")

    source = root / "docs/llms.txt"
    preamble, docs = parse(source.read_text(encoding="utf-8"))

    if not docs:
        print(f"error: no document links found in {source}", file=sys.stderr)
        return 1

    missing = [d["path"] for d in docs if not (root / d["path"]).is_file()]
    if missing:
        for path in missing:
            print(f"error: {source.name} links to {path}, which is not in this tree",
                  file=sys.stderr)
        return 1

    # Keep the preamble verbatim — it carries the summary and the version contract — but
    # retitle it, so an agent holding this file knows it is the full text, not the index.
    preamble = re.sub(r"\A# .*", "# Parsley — full documentation", preamble, count=1)

    parts = [
        preamble
        + "\n\nThis file is the full text of every document listed in llms.txt, in the same"
        + "\norder. It is generated from that file on each docs build; do not edit it by hand."
    ]
    for doc in docs:
        header = f"# {doc['title']}\n\nSource: {doc['url']}"
        if doc["blurb"]:
            header += f"\n\n{doc['blurb'].strip()}"
        body = (root / doc["path"]).read_text(encoding="utf-8").strip()
        parts.append(f"{header}\n\n{body}")

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(SEPARATOR.join(parts) + "\n", encoding="utf-8")
    print(f"{out}: {len(docs)} documents, {out.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
