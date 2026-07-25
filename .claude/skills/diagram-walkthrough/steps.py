#!/usr/bin/env python3
"""Index the two-channel topology sequence diagram by step number.

Mermaid's `autonumber` numbers every message in source order and nothing else, so the
Nth message line in the .mmd is step N in the rendered diagram. This script reproduces
that numbering, pairs each call with its matching return, and points at the class that
owns the callee.

Usage:
    steps.py --step 42            one step, with its call/return partner
    steps.py --range 42 60        a span of steps
    steps.py --phase 2            every step in a phase
    steps.py --grep stamp         steps whose label matches
    steps.py --list               every step, compact
    steps.py --participants       the participant to class map
    steps.py --step 42 --locate   also grep the owning class for the method
"""
import argparse
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
MMD = os.path.join(ROOT, "mermaid", "two-channel-topology", "two-channel-topology.mmd")
SRC = os.path.join(ROOT, "src", "main", "java", "io", "github", "tobyjamesclements", "parsley")

# Which class each participant stands for. `streams`, `c1`, `c2` and `c3` are not Parsley
# code: they are Kafka Streams and the broker, included so every call has a visible caller.
PARTICIPANTS = {
    "c1": ("c1-0 source channel", None),
    "c2": ("c2-0 source channel", None),
    "streams": ("Kafka Streams task thread", None),
    "p1": ("ParsleyProcessor", "ParsleyProcessor.java"),
    "user": ("the user's Processor delegate", None),
    "ctx": ("ParsleyProcessorContext", "ParsleyProcessorContext.java"),
    "l3": ("ParsleyGossip", "ParsleyGossip.java"),
    "l2": ("ParsleyCausalBroadcast", "ParsleyCausalBroadcast.java"),
    "l1": ("ParsleyChannels", "ParsleyChannels.java"),
    "reg": ("ParsleyOwnOutputRegistry", "ParsleyOwnOutputRegistry.java"),
    "c3": ("c3-0 sink channel", None),
}

MSG = re.compile(r"^(?P<src>\w+)(?P<arrow>--?>>)(?P<act>[+-]?)(?P<dst>\w+):\s*(?P<label>.*)$")
BANNER = re.compile(r"^Note over [^:]+:\s*(?P<text>\d\.\s.*)$")


def load():
    """Return (steps, phases). Step numbering mirrors mermaid autonumber."""
    steps, phases = [], []
    stacks, n = {}, 0
    for lineno, raw in enumerate(open(MMD), start=1):
        line = raw.strip()
        banner = BANNER.match(line)
        if banner:
            phases.append((n + 1, banner.group("text")))
            continue
        m = MSG.match(line)
        if not m:
            continue
        n += 1
        step = {
            "n": n, "line": lineno, "src": m.group("src"), "dst": m.group("dst"),
            "label": m.group("label").strip(), "partner": None,
            "kind": "return" if m.group("arrow") == "-->>" else "call",
        }
        # Pair calls with returns: `X->>+Y` pushes onto Y, `Y-->>-Z` pops Y.
        if step["kind"] == "call" and m.group("act") == "+":
            stacks.setdefault(step["dst"], []).append(step)
        elif step["kind"] == "return" and m.group("act") == "-":
            stack = stacks.get(step["src"]) or []
            if stack:
                opener = stack.pop()
                opener["partner"], step["partner"] = step, opener
        steps.append(step)
    return steps, phases


def phase_of(step, phases):
    label = "(before phase 1)"
    for start, text in phases:
        if step["n"] >= start:
            label = text
    return label


def locate(step):
    """Best-effort: grep the owning class for the identifiers in the label."""
    owner = PARTICIPANTS.get(step["dst"] if step["kind"] == "call" else step["src"])
    if not owner or not owner[1]:
        return ["    (not Parsley code, nothing to locate)"]
    path = os.path.join(SRC, owner[1])
    names = [n for n in re.findall(r"(\w+)\s*\(", step["label"]) if n != "new"]
    out = []
    for name in dict.fromkeys(names):
        try:
            hits = subprocess.run(
                ["grep", "-nE", r"\b" + re.escape(name) + r"\s*\(", path],
                capture_output=True, text=True, check=False).stdout.splitlines()
        except OSError:
            hits = []
        # Drop comment lines, then rank: a declaration opens a body, a call site ends in `;`.
        hits = [h for h in hits if not re.match(r"^\d+:\s*(\*|//)", h)]

        def rank(hit):
            body = hit.split(":", 1)[1].strip()
            if body.endswith("{"):
                return 0
            if body.endswith(";"):
                return 2
            return 1

        rel = os.path.relpath(path, ROOT)
        if hits:
            out.append("    %s -> %s" % (name, rel))
            for h in sorted(hits, key=rank)[:6]:
                body = h.split(":", 1)[1].strip()
                kind = {0: "decl", 2: "call", 1: "?"}[rank(h)]
                out.append("        %s:%-5s %-5s %s" % (rel, h.split(":", 1)[0], kind, body[:88]))
        else:
            out.append("    %s -> no match in %s" % (name, rel))
    return out or ["    (no method name in label)"]


def show(step, phases, args):
    owner_key = step["dst"] if step["kind"] == "call" else step["src"]
    owner = PARTICIPANTS.get(owner_key, (owner_key, None))
    arrow = "->>" if step["kind"] == "call" else "-->>"
    print("step %-4d %-6s %s %s %s" % (step["n"], step["kind"], step["src"], arrow, step["dst"]))
    print("    label:   %s" % step["label"])
    print("    phase:   %s" % phase_of(step, phases))
    print("    .mmd:    mermaid/two-channel-topology/two-channel-topology.mmd:%d" % step["line"])
    print("    callee:  %s%s" % (owner[0], (" (%s)" % owner[1]) if owner[1] else ""))
    if step["partner"]:
        p = step["partner"]
        rel = "returns at" if step["kind"] == "call" else "call was"
        print("    %s step %d: %s" % (rel, p["n"], p["label"]))
    if args.locate:
        print("    source candidates:")
        for line in locate(step):
            print(line)
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step", type=int)
    ap.add_argument("--range", nargs=2, type=int, metavar=("FROM", "TO"))
    ap.add_argument("--phase", type=int)
    ap.add_argument("--grep")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--participants", action="store_true")
    ap.add_argument("--locate", action="store_true")
    args = ap.parse_args()

    if args.participants:
        for key, (desc, path) in PARTICIPANTS.items():
            print("%-8s %-32s %s" % (key, desc, path or "-"))
        return

    if not os.path.exists(MMD):
        sys.exit("diagram not found at %s" % MMD)
    steps, phases = load()

    if args.list:
        for s in steps:
            print("%4d  %-6s %-8s %-8s %s" % (s["n"], s["kind"], s["src"], s["dst"], s["label"][:88]))
        print("\n%d steps. Phases start at: %s" % (
            len(steps), ", ".join("%d (%s)" % (n, t.split(".")[0]) for n, t in phases)))
        return

    if args.step:
        sel = [s for s in steps if s["n"] == args.step]
    elif args.range:
        sel = [s for s in steps if args.range[0] <= s["n"] <= args.range[1]]
    elif args.phase:
        bounds = [n for n, _ in phases]
        if args.phase < 1 or args.phase > len(bounds):
            sys.exit("phases are 1..%d" % len(bounds))
        lo = bounds[args.phase - 1]
        hi = bounds[args.phase] - 1 if args.phase < len(bounds) else len(steps)
        sel = [s for s in steps if lo <= s["n"] <= hi]
    elif args.grep:
        sel = [s for s in steps if args.grep.lower() in s["label"].lower()]
    else:
        print("%d steps. Phases start at: %s" % (
            len(steps), ", ".join("%d (%s)" % (n, t.split(".")[0]) for n, t in phases)))
        print("Pick one with --step N, or see --help.")
        return

    if not sel:
        sys.exit("no matching step")
    for s in sel:
        show(s, phases, args)


if __name__ == "__main__":
    main()
