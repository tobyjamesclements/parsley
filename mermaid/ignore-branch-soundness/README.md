# Why the gate's ignore branch is sound

The [delivery gate](../../docs/foundations/delivery-gate.md) is two branches and it is total:

```
for each coordinate c in deps:
    if consumed(c):  require frontier(c) >= deps[c]   # local delivery, never hearsay
    else:            ignore                            # counted by a metric, never a failure
```

The second branch is the one that needs an argument. Ignoring a dependency looks like the
unsound thing a causal protocol must never do. This diagram walks the case that proves it is
sound, following one causal chain across four nodes with different consumption sets.

## The scenario

`m1 → m2 → m3`, where the middle hop travels a channel the receiver never subscribes to.

| Node | Consumes | Produces |
|---|---|---|
| `p1` | nothing, a plain producer | `c1` |
| `p2` | `c1` | `c4` |
| `p3` | `c4` | `c2` |
| `p4` | `c1` and `c2`, **never `c4`** | — |

`p4` is the node under examination. When `m3` reaches it carrying a dependency on `c4-0 @4`,
`p4` has no subscription that could ever satisfy that coordinate. Waiting would deadlock.
Ignoring is what the gate does, and the diagram shows why nothing is lost by it.

## What each phase establishes

1. **The chain is built.** `p2` delivers `m1` and stamps `m2` with `m1`'s own coordinate. That is
   [I2](../../docs/foundations/invariants.md), stamp transitive completeness: a stamp dominates the
   dependency clocks *and the coordinates* of every event the node delivered.
2. **`p3` ignores but does not forget.** `p3` consumes no `c1`, so its own gate ignores `c1-0 @7`.
   Its merge keeps it anyway, and re-emits it on `m3`. That is
   [I9](../../docs/foundations/invariants.md), unconditional merge, the cross-node inductive step:
   the gate may ignore, the merge may not.
3. **`p4` evaluates, and holds.** The `c4-0` entry falls to the ignore branch. The `c1-0` entry
   falls to the consumed branch and is unsatisfied, because `p4`'s own contiguous frontier stands
   at `@6`. `m3` waits. A claim carried on a sibling channel never satisfies the gate, only local
   delivery does.
4. **The cause arrives and releases the effect.** `p4` polls `c1`, delivers `m1`, advances its
   frontier to `@7`, and `m3` releases behind it. Causal order preserved, with no coordination and
   no reference to `c4` at all.

The closing note gives the counterfactual: had `p3` dropped the out-of-scope entry, `m3` would
carry `{c4-0: 4}` alone, `p4` would ignore its only dependency and deliver `m3` ahead of `m1`. An
effect before its cause. The ignore branch is sound *only* in the presence of unconditional merge.

## Arrow conventions

- `->>` is an append to a topic-partition, or a node acting on its own state.
- `-->>` is a poll returning a record to a consumer.
- A self-directed arrow is a decision the node takes locally, gate evaluation or a merge.
- Numbering is mermaid `autonumber`, so step N here is step N in `steps.py`.

The counterfactual is deliberately a note rather than a crossed arrow. `steps.py` matches only
`->>` and `-->>`, but mermaid's `autonumber` counts `--x` as well, so drawing the broken path
would desync the script's numbering from the rendered diagram.

## How this diagram differs from the others

`two-channel-topology` is a **call-level** diagram: every arrow is a real Java method call with a
file and line behind it. This one is **protocol-level**: the participants are nodes and
topic-partitions, not classes, and the arrows are records moving between them. `steps.py --locate`
correctly reports nothing to locate for every participant.

It illustrates an argument from `docs/foundations/delivery-gate.md` rather than a code path. The
mechanism it depicts lives in `ParsleyCausalBroadcast` (the gate) and `ParsleyChannels` (the merge
and the contiguous frontier), covered by units 9 to 17 of the course.
