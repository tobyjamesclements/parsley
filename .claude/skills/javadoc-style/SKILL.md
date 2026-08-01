---
name: javadoc-style
description: Javadoc conventions for the Parsley codebase — JDK-style summary
  fragments, caller-facing content only, no promotional register, no em dashes
  or stray semicolons, complete @param/@return/@throws. Use this skill whenever
  writing or editing Javadoc, adding or changing a public type or method,
  reviewing a diff that touches doc comments, or when asked to document Java
  code. Consult it even for a single one-line comment.
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
what to do when a capability is absent. Cut the argument for why the design is
correct.

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
- **Unverifiable claims** about thread safety, ordering, or performance. If it
  is not enforced in code you can see, cut it.

## Tags

Missing tags are a worse defect than bad prose, and easier to miss because the
prose looks finished.

- Every declared or documented `throw` gets an `@throws` naming the condition.
- Every non-void method gets an `@return`, unless the summary line already
  states the return exactly.
- Every parameter with a constraint gets an `@param` stating it.
- Tags are lowercase noun phrases, not sentences, and take no trailing period.
- Use `{@link}` instead of restating what another type does.
- `{@link}` resolves through the file's imports. If you reference a type the
  file does not import, either add the import or fully qualify it.

## State a constraint once, where it is enforced

Do not repeat in a class comment what a factory method's `@throws` already says.

## Examples

Real before/after pairs from this codebase.

---

### 1. Metaphor, packed summary, colon into a non-list

**Before**

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

**Before**

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

**Before**

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

**Before**

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

**Before**

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

---

### 6. Fragment start

**Before**

```java
/** Registers a state-transition listener. Before {@link #start()}. */
```

**After**

```java
/** Registers a state-transition listener. Call before {@link #start()}. */
```

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

**Existing violation:** `explainHolds()` currently ends with a reference to
`docs/guide/diagnosing-holds.md`. Replace it with the actionable instruction
alone:

```java
 * <p>A held record means a cause is missing, lagging, or unstamped. Deliver the
 * cause. Do not skip, reorder, or add a timeout. Each
 * {@link HeldRecord.Diagnosis} names its case.
```

The walkthrough stays on the docs site. Readers who need it will find it from
there, and the caller who only needs to know "deliver the cause" is served
without leaving the tooltip.

### Adding a page

Any new page under `docs/` needs a `nav` entry in `mkdocs.yml`. A page that
builds but is unreachable from the nav is worse than no page.
