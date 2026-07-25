---
name: diagram-walkthrough
description: Walk the two-channel topology sequence diagram call by call, pointing at the file and line each call lives at and explaining the method, its parameters and why it matters. Use when reading mermaid/two-channel-topology/ or learning how the three protocol layers interact at runtime.
---

# Walking the topology sequence diagram

`mermaid/two-channel-topology/` traces one Parsley task through init, a held record, the release
cascade, and an inbound null message. Every arrow is a real call. This skill turns the diagram into
a guided read of the source, one call at a time, so the reader ends up in the code rather than
looking at a picture of it.

The diagram is numbered with mermaid `autonumber`, so every arrow has a step number. Those numbers
are the currency of the walkthrough.

## 1. Ask where to start, before explaining anything

This is the required first action. Do not begin narrating at step 1 by default.

Run `steps.py` with no arguments first, because it prints the current step count and the step number
each phase starts at. Never quote phase boundaries from memory, since they move whenever the diagram
is edited.

```
python3 .claude/skills/diagram-walkthrough/steps.py
```

Then ask the reader where to begin, offering the four phase entry points it just printed as the
options, and make clear they can name any step number instead. If they name a method or a concept
rather than a number, find it with `--grep` and offer the matching steps.

## 2. The index

`steps.py` reproduces mermaid's numbering by counting message lines in the `.mmd`, so step N in the
script is step N in the rendered diagram. It also pairs each call with its matching return and names
the class that owns the callee.

```
steps.py --step 62 --locate     one step, with candidate source lines
steps.py --range 62 77          a span
steps.py --phase 3              a whole phase
steps.py --grep stamp           steps whose label mentions stamp
steps.py --list                 every step, compact
steps.py --participants         the participant to class map
```

`--locate` greps the owning class and marks each hit `decl` or `call`. It is a pointer, not an
answer. Open the file and confirm before telling the reader a line number.

## 3. Explaining one call

Cover these, in this order. Read the method first. Never explain a call from its diagram label
alone, because the label is a summary and the source is the fact.

1. **Where you are.** Step number, caller to callee, and the phase it sits in.
2. **Where it lives.** `path/to/File.java:LINE` for the declaration, verified by reading it.
3. **The signature.** Copied from the source, not paraphrased.
4. **The parameters.** Each one: what it is, and where that value came from earlier in the
   diagram. A reader who cannot trace an argument back to its origin has lost the thread.
5. **The return.** What comes back, and the step number that carries it. `steps.py` gives the
   paired step.
6. **Why it matters.** What this call establishes for the protocol. Where the Javadoc cites a
   named invariant, cite the same one, and say what it means rather than only its number.
7. **What breaks without it.** The concrete failure. Prefer the reasoning the Javadoc and the
   inline comments already give, since several of them record a bug that was actually hit.

Then stop and let the reader steer.

## 4. Rules that keep the walkthrough honest

- **The code is authoritative.** If a diagram label disagrees with the source, say so plainly and
  call it a defect in the diagram. Do not narrate the diagram's version as if it were true.
- **Do not invent rationale.** These classes carry dense Javadoc explaining why each step is
  ordered as it is. Quote and explain that. If the source gives no reason, say the reason is not
  recorded rather than supplying a plausible one.
- **Treat a call and its return as one unit.** They are two step numbers but one event.
- **Say what the non-Parsley participants stand for.** `c1`, `c2` and `c3` are topic-partitions,
  `streams` is the Kafka Streams task thread, and `user` is the caller's own processor. `--locate`
  reports these as having nothing to locate, which is correct rather than a gap.
- **A self-directed arrow is still a call.** Locate it like any other.

## 5. Pace

Default to one call at a time, then ask whether to continue, jump elsewhere, or go deeper on what
was just covered. Offer to summarise a whole phase first if the reader wants the shape before the
detail. Some phases run to sixty steps, so marching through one without checking in is rarely what
the reader wants.

## 6. Background worth having open

- `mermaid/two-channel-topology/README.md` for the scenario, the arrow conventions, and why the
  deferred producer acknowledgement sits between phases rather than under the send.
- `docs/protocols/index.md` for the three modules and the end-to-end flow.
- `docs/foundations/invariants.md` for I1 to I9, which the Javadoc cites by number.
- `docs/reference/processor.md` for processor init and the stamping proxy.
