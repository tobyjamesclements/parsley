---
name: javadoc-style
description: Javadoc conventions for the Parsley codebase — JDK-style summary
  fragments, caller-facing content only, no promotional register, no em dashes
  or stray semicolons, complete @param/@return/@throws, record and type-parameter
  tags, and the one-line property style for @Test comments. Use this skill
  whenever writing or editing Javadoc, adding or changing a public type or
  method, adding a test, reviewing a diff that touches doc comments, or when
  asked to document Java code. Consult it even for a single one-line comment.
---

# Javadoc style

## The governing question

Javadoc answers **"how do I use this correctly?"** It does not answer "why is
it built this way?"

Design rationale, rejected alternatives, and arguments for the shape of the API
belong in the mkdocs site under `docs/`. If a comment would only satisfy someone
reviewing the design, it is in the wrong file.

The distinction is not "cut the technical detail". Keep any fact a caller could
act on or be burned by: staleness windows, rollback visibility, forced config,
thread confinement, what to do when a capability is absent. Cut the argument for
why the design is correct.

## Three tiers, three standards

The governing question above is the **public API** standard. It is not the only
one in this tree, and applying it everywhere destroys information.

| Tier | What it is | Standard |
|---|---|---|
| **Public API** | `public` types and members in `src/main/java` | Everything in this skill. Ships to javadoc.io and IDE tooltips. |
| **Internals** | package-private types and members (`CausalNode`, `VectorClock`, `Stage`'s adapter, `Holds`, `Positions`) | Register rules apply. The "no rationale" rule does **not**. |
| **Tests** | `src/test/java` | The one-line property style below. |

### Internals keep their reasoning

`CausalNode` documents why the normalisation step is sound and why the wait
graph is acyclic. That is not decoration. `CLAUDE.md` requires a vendored copy
to be verbatim precisely because these invariants "fail silently when
paraphrased" — the comment is how the next maintainer knows which line is load
bearing.

So in package-private code:

- Keep invariants, soundness arguments, and the reason a step is ordered where
  it is.
- Do not "clean up" a paragraph that explains why something is safe. If it
  reads as ceremony, check whether it is stating an invariant before cutting.
- Register rules still apply. No promotional adjectives, no metaphor, no
  colons outside lists.
- The no-docs-site-references rule (below) still applies. Name the concept
  instead of the path: "the closed-effects precondition" is searchable and
  never rots.

Package-private members do not appear in the published Javadoc, so nothing here
is a tooltip concern. It is a maintainer concern, and the maintainer is reading
the file.

## Summary line

The first sentence is what lands in the class and method index tables, and in
IDE tooltips. Anything after the first period is invisible there. Put the whole
answer in it.

- Third-person verb fragment, no subject, ends with a period.
  `Composes {@link Stage}s into a runnable pipeline.`
  `Returns records currently held at the gate, longest-held first.`
- Never `This method composes...`, never `A class which composes...`.
- Never a bare noun phrase used as a label. `The composed stages, in
  declaration order.` should be `Returns the composed stages, in declaration
  order.`
- Never a sentence fragment doing sentence work. `Before {@link #start()}.`
  should be `Call before {@link #start()}.`
- A comment that is all tags and no summary is incomplete. `CausalClock`'s
  constructor is the current instance: it opens straight into `@param`.

**Builder setters attract the label form.** `How often the log-start truncation
sweep runs. Default ten minutes.` is a noun phrase pretending to be a summary.
Write `Sets how often the log-start truncation sweep runs.` and keep the default
in the following sentence or in the `@param`. There are several of these across
`Stage.Builder` and `Stage.Stateful`.

## Register

Document what a caller must know. Prefer five words to twenty. Delete any
sentence a caller cannot act on.

### Banned

- **Em dashes.** Almost always a parenthetical that should be its own sentence.
- **Semicolons.** Two sentences.
- **Colons**, except immediately before a genuine list. A `<ul>` or a real
  enumeration qualifies. `Assembles the runtime: forces X, installs Y` does not.
- **Metaphor standing in for a plain statement:** "the front door", "under the
  hood", "escape hatch", "footgun".
- **Evaluative or promotional words:** powerful, robust, seamless, elegant,
  efficient, simply, easily, gracefully, cleanly, first-class, out of the box.
- **Filler openers:** "Note that", "It is important to", "Keep in mind",
  "Be aware".
- **Restating the identifier:** "The explainHolds method explains which records
  are held."
- **Rhetorical triples and parallel flourishes:** "fast, safe, and correct";
  "a minimal surface can grow compatibly; a regretted member cannot".
- **Announcing structure instead of stating it:** "the two ways to run it",
  "the withheld members are withheld for different reasons".
- **References to the docs site.** No paths, no URLs, no "see the guide". See
  the rule below.

### Concurrency and performance: state, do not promise

Do not claim a guarantee the code does not establish. "Thread-safe", "lock-free",
"O(1)", "no allocation" are claims a caller will build on, and nothing enforces
them.

A **constraint on the caller** is different, and must be stated:
`CausalClock` says "Not thread-safe. One instance per producing thread." That is
a usage precondition a caller gets wrong at their cost, and it is verifiable by
reading the class. Keep it. Fix the punctuation, not the fact.

The test: does the sentence tell the caller what they must do, or tell them what
they may assume? State the first. Cut the second unless the code in front of you
establishes it.

## Tags

Missing tags are a worse defect than bad prose, and easier to miss because the
prose looks finished. `javadoc -Xdoclint:all` currently reports 101 of them in
`src/main/java` (61 `@param`, 23 `@return`, 15 undocumented members). They are
warnings, not errors, so no build will catch yours.

- Every declared or documented `throw` gets an `@throws` naming the condition.
- Every non-void method gets an `@return`, unless the summary line already
  states the return exactly.
- Every parameter with a constraint gets an `@param` stating it.
- **Every type parameter gets an `@param <T>`.** `Codec<T>`, `Topic<K, V>`,
  `Fold<S, K, V>`, `Step<S>` and `Message<K, V>` are all missing theirs. State
  what the parameter ranges over, not its letter: `@param <S> the stage's
  per-key state type`.
- Tags are lowercase noun phrases, not sentences, and take no trailing period.
- Use `{@link}` instead of restating what another type does.
- `{@link}` resolves through the file's imports. If you reference a type the
  file does not import, either add the import or fully qualify it.
- Use `{@value #CONST}` rather than repeating a constant's literal, as
  `CausalHeaders` does for the header names.

### Records

A record's components are documented with `@param` on the **type**, never with
comments on the generated accessors. `HeldRecord` and `Tick` do this correctly
and are the models to copy. `Message`, `Step` and `ContractProbes.Finding`
currently document none of theirs.

A compact constructor that validates or copies gets an `@throws` if it can
throw. `HeldRecord`'s copies its list, so it needs none.

### Functional interfaces

The single abstract method usually carries no comment of its own, because the
type comment holds the contract (`Handler`, `Fold`, `TickHandler`, `TickFold`,
`Codec` all do this). That is fine when the type comment genuinely states the
parameters and the return. If it does not, put the tags on the method rather
than leaving both empty.

### Enum constants

Each constant gets its own comment saying what condition it represents and what
the reader should do about it. `HeldRecord.Diagnosis` is the model.

### Parallel members must not diverge

`Stage.Builder` and `Stage.Stateful` mirror each other. Where the same method
appears on both, the two comments must state the same contract. Today
`holdWarningAfter` documents the disable-by-zero behaviour on one and abbreviates
it on the other. Either state it fully in both places or state it fully in one
and make the other's summary point there with `{@link}`.

## State a constraint once, where it is enforced

Do not repeat in a class comment what a factory method's `@throws` already says.

## Formatting

- Wrap doc comment lines at 96 columns, matching the surrounding source.
- `<p>` opens each paragraph after the first. No closing `</p>`.
- Indent `<li>` content inside `<ul>` by four columns, as `VectorClock` does.

## Examples

Real before/after pairs from this codebase.

---

### 1. Metaphor, packed summary, colon into a non-list

**Before** (`Parsley`)

```java
/**
 * The front door: a composition of {@link Stage}s, and the two ways to run it — a
 * broker-less topology for {@code TopologyTestDriver}, or a {@link CausalStreams}
 * runtime against a real cluster.
 */
```

**After**

```java
/**
 * Composes {@link Stage}s into a runnable pipeline.
 *
 * <p>Run broker-less against {@code TopologyTestDriver} with {@link #testTopology()},
 * or on a cluster with {@link #streams(Properties)}.
 */
```

Summary line goes from 33 words to 6. The metaphor, the colon, the em dash and
the structural announcement all disappear together, because they were all doing
the same job: deferring the actual statement.

---

### 2. Design defence in a class comment

**Before** (`CausalStreams`)

```java
/**
 * The running application: a curated view of the Kafka Streams runtime [...] The
 * surface is an allowlist — each member is present because it is causally inert —
 * and there is no accessor to the underlying {@code KafkaStreams}, ever: any single
 * escape hatch would reintroduce the full inherited surface. The withheld members
 * are withheld for different reasons, each with its operational alternative:
 *
 * <ul>
 * <li>[...] Thread add/remove and {@code cleanUp()} are causally inert and withheld
 * only to keep the surface minimal [...] A minimal surface can grow compatibly; a
 * regretted member cannot be removed compatibly.</li>
 * </ul>
 */
```

**After**

```java
/**
 * A running Parsley application, created by {@link Parsley#streams(Properties)}.
 *
 * <p>Exposes a restricted subset of {@link KafkaStreams}, with no accessor for the
 * underlying instance. Where a member is absent, use the alternative:
 *
 * <ul>
 *   <li>Interactive queries and {@code store()}: observe state through sinks. Under
 *       exactly-once semantics a local store holds the open transaction's writes, so
 *       a query can observe state that is not a function of any delivered history.</li>
 *   <li>{@code pause()} and {@code resume()}: stop the instance, or scale by
 *       instances. Pausing one instance freezes its release punctuator and stalls
 *       every instance waiting on its output.</li>
 *   <li>Thread add/remove and {@code cleanUp()}: scale by instances; delete the state
 *       directory of a stopped instance.</li>
 * </ul>
 */
```

~250 words to ~100. **Keep the alternative, cut the justification.** The
compatibility argument belongs on the docs site. Note the colons here are
legitimate: each introduces the alternative for a listed item.

---

### 3. Justifying inclusion rather than describing behaviour

**Before** (`CausalStreams.explainHolds`)

```java
 * <p>It is present on this allowlist where {@code store()} is not, because it is
 * causally and operationally inert. It returns coordinates, claims, and
 * watermarks — never key or value bytes, so it cannot become a back door to state
 * a query would have observed — it reads only a snapshot each task published from
 * its own stream thread [...]
```

**After**

```java
 * <p>Returns coordinates, claims, and watermarks, never key or value bytes. Reads a
 * snapshot each task publishes from its own stream thread, refreshed on the
 * wall-clock punctuator. A reading may be that stale, and describes an in-flight
 * view that an aborted transaction may roll back.
```

The staleness and rollback facts stay: those are correctness hazards for the
caller. The allowlist argument goes.

---

### 4. Semicolon hiding two missing tags

**Before** (`CausalStreams.close`)

```java
/** Bounded shutdown; {@code false} when the timeout elapsed first. */
public boolean close(Duration timeout) {
```

**After**

```java
/**
 * Closes within {@code timeout}.
 *
 * @param timeout how long to wait for clean shutdown
 * @return {@code false} if the timeout elapsed first
 */
public boolean close(Duration timeout) {
```

The semicolon was compressing two facts into one line so neither got a tag. The
punctuation problem and the contract problem were the same problem.

---

### 5. Undocumented throws on a public factory

**Before** (`Parsley.of`)

```java
public static Parsley of(Stage... stages) {
```

(no comment; throws `IllegalArgumentException` on three distinct conditions)

**After**

```java
/**
 * Composes stages into a pipeline.
 *
 * @param stages the stages, in declaration order
 * @throws IllegalArgumentException if no stages are given, if two stages share a
 *         name, or if a topic is a source of more than one stage
 */
public static Parsley of(Stage... stages) {
```

`Stage.Builder.ticks`, `Stage.Stateful.ticks` and both `build()` methods have the
same defect.

---

### 6. Fragment start

**Before** (`CausalStreams.setStateListener`)

```java
/** Registers a state-transition listener. Before {@link #start()}. */
```

**After**

```java
/** Registers a state-transition listener. Call before {@link #start()}. */
```

---

### 7. A caller constraint that survives the register rules

**Before** (`CausalClock`)

```java
 * <p>A producer that stamps nothing claims nothing — safe, and the reason adoption
 * needs no flag day. Not thread-safe; one instance per producing thread.
```

**After**

```java
 * <p>A producer that stamps nothing claims nothing. Not thread-safe. Use one
 * instance per producing thread.
```

The em dash, the semicolon and the adoption argument go. The confinement
requirement stays, because a caller who shares an instance across threads
corrupts the clock.

## Test Javadoc

`CLAUDE.md` requires Javadoc on every `@Test`. Tests follow a different summary
convention from API members, and the API rules above must not be applied to them.

- **A test comment states the property being asserted, as a declarative
  sentence.** `Watermarks never regress: advanceTo and mergeMax are max-folds.`
  Not `Tests that watermarks never regress.` and not a verb fragment.
- One line, one sentence, on the same `/** ... */` line where it fits.
- Name the property, not the mechanics. `Delivering the missing cause releases
  the hold, and the explanation empties with it.` beats `Feeds a record, then
  feeds its cause, then checks the list is empty.`
- A colon introducing the mechanism after the property is idiomatic here and is
  the one place the colon rule relaxes: `Dominance is pointwise, with absent
  channels claiming nothing.` or `Truncation drops entries at or below the
  stability bound, and only those.`
- The class comment says what surface the file covers and how the tests get at
  it. `ExplainHoldsTest` is the model.
- Test helper methods get the same one-line treatment when their role is not
  obvious from the name.
- Assertion messages are separate from Javadoc and still required.

## Where cut content goes

`docs/` is an mkdocs site. It is the destination for everything Javadoc should
not carry.

- **Rationale, rejected alternatives, compatibility arguments** → a page under
  `docs/`. Prefer an existing page over a new one. If none fits, propose the
  new page and its `nav` entry rather than creating it silently.
- **Entry-point orientation and package-level narrative** →
  `package-info.java`, which is still Javadoc and still follows these rules.
- **Operational walkthroughs** → `docs/guide/`, following the existing
  `diagnosing-holds.md` pattern.
- **Protocol invariants and soundness arguments** → stay in the package-private
  source, per the tier table. `docs/design/` carries the same material for a
  reader who is not in the file.

### Javadoc never references the docs site

**Hard rule. No exceptions.**

- No `{@code docs/guide/...}` path references.
- No `<a href>` to the published site.
- No "see the guide", "documented in the user guide", or any variant.
- No `@see` pointing anywhere outside the Java source.

Javadoc ships inside the jar and is read on javadoc.io, in IDE tooltips, and
from Maven Central, all detached from the repository and from any published
site. A repo-relative path is dead text in every one of those places, and a URL
rots the moment the site is restructured. Javadoc is a self-contained artifact.

This means each doc comment must stand alone. If a caller needs a fact to use
the member correctly, state it in the comment. If the fact is too long to state,
the comment is carrying explanatory material that belongs on the docs site
instead, and the fix is to cut it, not to link to it.

The traffic goes one way. The docs site may link into the published Javadoc as
much as it likes.

#### This rule is about comment prose, not about string values

Several public members **return** documentation anchors as data, by design:

- `HeldRecord.Diagnosis.reference()` and the anchors passed to its constructor.
- `ContractProbes`'s `CLAUSE_*` constants and `ContractProbes.Finding.clause()`.
- `HeldRecord.Unmet.describe()` and `HeldRecord.summary()`, which embed the
  anchor in the line an operator reads in the log.

These are the runtime contract. `ExplainHoldsTest` asserts on them. Do not
delete them while cleaning up comments, and do not treat a Javadoc `@return`
that describes one as a violation — `/** The documentation anchor that explains
this diagnosis in full. */` is correct, because it describes a value rather than
sending the reader somewhere.

#### Current violations

Six sites, all in comment prose, all in public API:

| File | What to do |
|---|---|
| `HeldRecord` class comment | Drop the trailing `see docs/guide/diagnosing-holds.md`. The rule sentence before it already carries the instruction. |
| `HeldRecord.Diagnosis` enum comment | Drop `({@code docs/foundations/liveness.md})`. Keep the claim it supports: every wait resolves once its cause is delivered. |
| `CausalStreams.explainHolds` | See the replacement below. |
| `Handler` class comment | `(the closed-effects precondition, {@code docs/...})` becomes `(the closed-effects precondition)`. The named concept is searchable. |
| `ContractProbes` class comment | `Executable checks for the application contract (docs/guide/expectations.md)` becomes `Executable checks for the application contract`. |
| `package-info` | Drop the trailing `see {@code docs/reference/conformance-kit.md}`. Name the `test-jar` artifact, which the sentence already does. |

The replacement for `explainHolds`:

```java
 * <p>A held record means a cause is missing, lagging, or unstamped. Deliver the
 * cause. Do not skip, reorder, or add a timeout. Each
 * {@link HeldRecord.Diagnosis} names its case.
```

The walkthrough stays on the docs site. Readers who need it will find it from
there, and the caller who only needs to know "deliver the cause" is served
without leaving the tooltip.

None of these six touch a test. `ExplainHoldsTest`'s assertion on
`docs/guide/diagnosing-holds.md` reads `summary()`, which is built from
`Diagnosis.reference()` — a string value, not a comment.

### Adding a page

Any new page under `docs/` needs a `nav` entry in `mkdocs.yml`. A page that
builds but is unreachable from the nav is worse than no page.

## Checking your work

```
javadoc -Xdoclint:all -d /tmp/jd -cp "$(./mvnw -q dependency:build-classpath \
  -Dmdep.outputFile=/dev/stdout -DincludeScope=compile | tail -1)" \
  src/main/java/io/github/tobyjamesclements/parsley/*.java
```

Missing tags are warnings, so the count only matters relative to where you
started. Do not leave it higher than you found it. `./mvnw javadoc:javadoc`
builds the site copy and is what the Pages workflow runs, but it does not
surface these warnings.

Prose rules have no tool. Read the summary line back on its own and ask whether
it answers the question a caller arrived with.
