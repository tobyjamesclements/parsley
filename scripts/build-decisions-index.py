#!/usr/bin/env python3
"""Generate docs/decisions.md: an index of DECISIONS.md with each record's later citations.

DECISIONS.md is append-only. A record that a later record supersedes, corrects or extends
keeps its text unchanged, so a reader who lands on it alone cannot tell whether it still
stands; the header rule says a superseded record "says which record supersedes it", and in
practice only a few headings do. This index derives that map from the log itself: for every
record, the later records whose text cites it by number, with the relation their wording
names where one is recognisable (supersedes, corrects, extends, amends, refutes, retires,
narrows, closes). It is generated at docs-build time, like llms-full.txt, so it cannot
drift from the log; nothing in DECISIONS.md is edited.

A citation is evidence that a later record bears on this one, not proof that it is dead:
follow the link and read. A record cited by nothing later is, as far as the log knows,
current.

Usage: scripts/build-decisions-index.py [repo-root] [output-path]
"""

import re
import sys
from pathlib import Path

HEADING = re.compile(r"^### D(?P<number>\d+) — (?P<title>.+)$")
CITATION = re.compile(r"\bD(?P<number>\d+)\b")
RELATION = re.compile(
    r"\b(?P<verb>supersed\w*|correct\w*|extend\w*|amend\w*|refut\w*|retir\w*|narrow\w*|close[sd]?|"
    r"resolv\w*|revers\w*|implement\w*|qualif\w*|tighten\w*|widen\w*)\b",
    re.IGNORECASE,
)
RELATION_LABEL = {
    "supersed": "supersedes",
    "correct": "corrects",
    "extend": "extends",
    "amend": "amends",
    "refut": "refutes",
    "retir": "retires",
    "narrow": "narrows",
    "close": "closes",
    "resolv": "resolves",
    "revers": "reverses",
    "implement": "implements",
    "qualif": "qualifies",
    "tighten": "tightens",
    "widen": "widens",
}
GITHUB_BLOB = "https://github.com/tobyjamesclements/parsley/blob/main/DECISIONS.md"


def parse(text: str):
    """Split the log into records: (number, title, heading line number, body lines)."""
    records = []
    current = None
    for line_number, line in enumerate(text.splitlines(), start=1):
        match = HEADING.match(line)
        if match:
            current = {
                "number": int(match.group("number")),
                "title": match.group("title").strip(),
                "line": line_number,
                "heading": line,
                "body": [],
            }
            records.append(current)
        elif current is not None:
            current["body"].append(line)
    return [r for r in records if r["number"] > 0]  # D0 is the template


def label(verb: str) -> str:
    verb = verb.lower()
    for stem, shown in RELATION_LABEL.items():
        if verb.startswith(stem):
            return shown
    return verb


def relation(sentence: str, cited: int) -> str:
    """The relation a sentence names for its citation of D<cited>, or '' if none is recognisable.

    Only a verb within a few words of the citation counts, so a sentence that corrects one
    record while merely mentioning another does not label both.
    """
    for match in re.finditer(rf"\bD{cited}\b", sentence):
        window = sentence[max(0, match.start() - 40):match.end() + 40]
        for verb in RELATION.finditer(window):
            # "a superseded execution" is the protocol's term for a zombie, not a relation
            # between records.
            if re.match(r"\s+(execution|lifetime|instance)", window[verb.end():]):
                continue
            return label(verb.group("verb"))
    return ""


def citations(records):
    """Map each record number to the later records citing it: {cited: [(citing, relation)]}."""
    cited_by = {r["number"]: [] for r in records}
    for record in records:
        seen = {}
        # The heading's parenthetical is the record's own statement of its relations and
        # wins over anything the body says.
        heading_tail = record["heading"].split("—", 1)[1] if "—" in record["heading"] else ""
        for match in CITATION.finditer(heading_tail):
            cited = int(match.group("number"))
            if cited < record["number"] and cited in cited_by:
                seen[cited] = relation(heading_tail, cited)
        for sentence in re.split(r"(?<=[.;:])\s+", " ".join(record["body"])):
            for match in CITATION.finditer(sentence):
                cited = int(match.group("number"))
                if cited >= record["number"] or cited not in cited_by:
                    continue
                named = relation(sentence, cited)
                if cited not in seen or (not seen[cited] and named):
                    seen[cited] = named
        for cited, named in seen.items():
            cited_by[cited].append((record["number"], named))
    return cited_by


def render(records, cited_by) -> str:
    by_number = {r["number"]: r for r in records}
    lines = [
        "# Decisions index",
        "",
        "`DECISIONS.md` in the repository is append-only: a record that a later record supersedes,",
        "corrects or extends keeps its original text. This page, generated from the log at every",
        "docs build, lists each record with the later records that cite it and the relation their",
        "wording names, so a reader can tell what still stands before relying on it. A citation",
        "is a pointer to read, not a verdict; a record cited by nothing later is, as far as the",
        "log knows, current.",
        "",
        "| Record | Decision | Cited by later records |",
        "|---|---|---|",
    ]
    for record in records:
        number = record["number"]
        link = f"[D{number}]({GITHUB_BLOB}#L{record['line']})"
        citing = cited_by[number]
        if citing:
            rendered = ", ".join(
                f"[D{n}]({GITHUB_BLOB}#L{by_number[n]['line']})" + (f" ({rel})" if rel else "")
                for n, rel in sorted(citing)
            )
        else:
            rendered = "—"
        title = record["title"].replace("|", "\\|")
        lines.append(f"| {link} | {title} | {rendered} |")
    lines.append("")
    superseded = sorted(
        n for n, citing in cited_by.items()
        if any(rel in ("supersedes", "reverses") for _, rel in citing)
    )
    corrected = sorted(
        n for n, citing in cited_by.items()
        if n not in superseded and any(rel in ("corrects", "refutes", "amends") for _, rel in citing)
    )
    lines += [
        "## At a glance",
        "",
        f"{len(records)} records. Named as superseded or reversed by a later record: "
        + (", ".join(f"D{n}" for n in superseded) if superseded else "none")
        + ". Named as corrected, refuted or amended in part: "
        + (", ".join(f"D{n}" for n in corrected) if corrected else "none")
        + ". Every other citation is an extension or a reference; read the citing record to see"
        " which.",
        "",
    ]
    return "\n".join(lines)


def main(argv):
    root = Path(argv[1]) if len(argv) > 1 else Path(".")
    output = Path(argv[2]) if len(argv) > 2 else root / "docs" / "decisions.md"
    text = (root / "DECISIONS.md").read_text(encoding="utf-8")
    records = parse(text)
    if not records:
        sys.exit("DECISIONS.md: no records found")
    numbers = [r["number"] for r in records]
    if numbers != sorted(numbers) or len(set(numbers)) != len(numbers):
        sys.exit("DECISIONS.md: record numbers are not strictly increasing: " + str(numbers))
    output.write_text(render(records, citations(records)), encoding="utf-8")
    print(f"wrote {output} ({len(records)} records)")


if __name__ == "__main__":
    main(sys.argv)
