---
name: big-task
description: Workflow for large or multi-session changes - plan first, checkpoint the plan to disk, land one verified commit per task, clean up on completion. Use when starting any change too big for a single sitting or a single commit.
---

# Big-task workflow

A plan held only in conversation context is not recoverable; big tasks span context windows and
sessions. Follow this discipline for any change too large for one sitting or one commit.

## 1. Plan on disk before any code

Write the full plan to a working file `.claude/<task-name>-plan.md` (gitignored, machine-local):

- The goal and the design decisions, including alternatives considered and why they were
  rejected. Rejected alternatives matter: they stop a later session from re-deriving them.
- An ordered task list, each task small enough to land as one commit.
- A **Status** line kept current: branch, base commit, what is done, what is next. This is the
  recovery point if the session ends mid-task.

## 2. Get approval

Stress-test the plan before presenting it (restart/crash, multi-topic, serde/schema, and EOS
edge cases — see CLAUDE.md). Present it and wait for explicit approval before touching code.
State infeasibility plainly; a confidently wrong plan wastes the review.

## 3. Land one task at a time

Each task lands as one commit with:

- `mvn clean verify` green (the full gate: unit, broker ITs, PIT, NullAway);
- the `CHANGELOG.md` `[Unreleased]` entry for the change;
- the plan file's Status line updated —

all in the same commit's working state, so an interrupted session resumes from a consistent
point.

## 4. Carry the work in the main session

Continue across sessions by re-reading the plan file, verifying its claims against the code
(the file can go stale; code and tests are authoritative), and picking up the next task. Do not
delegate implementation to subagents; use them only for research and search fan-out.

## 5. Clean up on completion

Fold anything durable into its proper home — `docs/`, Javadoc, code comments, CLAUDE.md, or a
GitHub issue — then delete the plan file. The lasting record is git history and the CHANGELOG,
not the working file.
