# The contract

This page collects the working agreement in one place: what Parsley expects from you, what
it promises back, and what it deliberately does not promise. Each expectation restates a
precondition of [the causal model](../foundations/causal-model.md#preconditions), which
holds the formal versions; the links under each item lead to the reasoning.

## What Parsley expects of you

**Environment.**

- Java 21 or later, and brokers that serve topic IDs (3.7+) — channel identity is
  `(topicId, partition)`
  ([channels, coordinates, clocks](../foundations/causal-model.md#channels-coordinates-clocks)).
- `processing.guarantee=exactly_once_v2`, with `read_committed` consumption. `Parsley.streams`
  forces both; there is no at-least-once mode, because all of Parsley's state commits in the
  task's transaction.
- Run the topology through `Parsley.streams`. The adapter self-checks at runtime — the
  processing guarantee at init, captured consumer positions at the first delivered record —
  and fails closed when the topology is hosted any other way.

**Topics.**

- Every declared topic exists before the application starts. Sinks are resolved at init and
  a missing one fails loudly; nothing is auto-created, and there are no automatic
  repartition topics.
- Topic identity is stable mid-run: a topic deleted and recreated under the same name is a
  different channel, and the old incarnation's history is loss, never reordering.
- Retention covers consumer lag on causal topics. A consumer must be able to fetch, or
  observe the skip of, every offset a claim names; undersized retention fails loudly at the
  position reset, never silently.

**Keys and partitioning.**

- Co-partition a stage's source topics by key: related keys must land on the same partition
  number of every source, which in practice means the same key bytes and the same partition
  count. The guarantee is per task, and Parsley, like Kafka Streams, does not enforce this
  precondition ([the causal model](../foundations/causal-model.md#preconditions)).
- Key encodings are canonical and stable — the encoded bytes choose the partition and key
  the fold state ([key codecs are identity](codecs.md#key-codecs-are-identity)).

**Logic.**

- Handlers and folds are pure, and their effects are closed: everything causally significant
  leaves through the returned emissions and fold state. A write to an external system, a
  send through a private producer, or a mutation of shared state is invisible to the causal
  model — no stamp claims it and no gate can order against it.
- Emissions name declared sinks only; an emission to an undeclared sink fails loudly at the
  stamping site.

**Names.**

- A stage's name keys its state stores and derives its sender identity, so it stays stable
  across deployments of the same application, as does the application id.

**Stamps.**

- Every producer on a causal path stamps its outputs. Stages do this automatically; plain
  producers use a [`CausalClock`](clients.md). An unstamped record claims nothing — safe for
  the record itself, invisible to downstream ordering.

## What Parsley promises

- **Causal delivery, per task.** A record reaches your logic only after every cause your
  stage consumes has been delivered locally, channels are FIFO per partition, and each
  `Message` carries its own coordinate — for a record released from the hold queue, its own,
  not the release trigger's.
- **One transaction per delivery.** The delivery, the fold state it wrote, and the emissions
  it caused commit atomically under exactly-once semantics, so crash recovery is a pure
  restore and a stateful stage's fold state is a deterministic function of the delivered
  history.
- **Send order is claimed.** The emissions of one delivery, and one stage-task's successive
  sends across all its sink topics, are claimed in send order — a stage that consumes
  several of your sinks delivers your outputs in the order you sent them
  ([the stamp](../design/architecture.md#the-stamp)).
- **Joining needs no coordination.** A new application starts consuming, self-gates into
  causal order during replay, and its stamps make its outputs correctly gated everywhere
  from its first emission.
- **Blocks or fails, never guesses.** There is no timeout that reorders, drops, or delivers
  early. The fail-closed conditions are loud task failures: an undecodable clock, sender, or
  seq header; an emission to an undeclared sink; a missing sink at init; a topology hosted
  outside `Parsley.streams`. A failed send aborts with its transaction, taking every claim
  on it along, and the retry refetches. What a failure in your own logic does, and how to
  model domain failures instead, is [error handling](error-handling.md).
- **Held records cost disk, not heap.** Hold queues are unbounded and live in the state
  store and its changelog, so a lagging cause channel grows state rather than exhausting
  memory, and a wedge is a loud stall, never a silent loss
  ([hold queues](../design/architecture.md#hold-queues-are-unbounded-and-disk-backed)).

## What Parsley does not promise

- **No total order.** Records with no causal relation deliver in arrival order, and the
  interleaving of concurrent records may differ between instances and between runs. The
  delivered history is one valid extension of the causal partial order; logic whose outcome
  must not depend on the choice is written commutative for concurrent inputs.
- **No ordering across partition slices.** The guarantee is per task, which is what makes
  co-partitioning a precondition rather than advice.
- **No ordering through side channels.** Effects that bypass emissions — external writes,
  private producers — carry no stamps and are outside the guarantee.
- **No gating for plain consumers.** A plain consumer may observe stamps
  ([plain clients](clients.md)), but a consumer that wants causal delivery order is by
  definition a causal stage: run it as one.
- **No backpressure surface.** Holding is the mechanism and monitoring is the interface;
  there is no per-channel pause, because the primary host cannot honour one.
- **No history below the baseline.** History below a node's first sighting of a channel,
  like history that retention has deleted, is outside scope by definition — a
  finite-retention fact, not an ordering exception.

## Operating it

- **Watch hold depth.** The `records-held` and `records-held-age-max-ms` gauges are the
  direct signals ([metrics](../reference/metrics.md)): the stage's committed positions
  advance past held records, so its own consumer lag does not show holding. A lagging or
  stalled cause channel also shows up as growth of the protocol state store (and its
  changelog) and as consumer lag on the cause topic; retention economics are the real bound
  on how long a record can wait.
- **A blocked head blocks its channel** (head-of-line blocking, by design). A producer whose
  stamps claim records its consumers cannot yet fetch convoys that channel; the cure is
  fixing the lagging cause, not skipping the head.
- **Truncation follows retention.** Stamp-side clock width is bounded by the causal history
  retention actually keeps; the sweep runs every `truncationInterval` (default ten minutes),
  and a failed sweep skips a cycle rather than failing the task, counting on
  `truncation-sweeps-skipped-total`
  ([truncation](../design/architecture.md#truncation-log-start-stability)).
- **Baseline late joiners at the last stable offset,** not the log end, when seeding a new
  consumer's start position by hand — every claim minted by an open transaction sits above
  the LSO, so it resolves
  ([the sequence-claim caveat](../foundations/liveness.md#the-sequence-claim-caveat-late-joiners)).
- **The runtime handle is an allowlist.** `CausalStreams` exposes lifecycle, listeners,
  state, metrics, and lag, and withholds members that can violate causality; the withheld
  members and their operational alternatives are on its Javadoc and in
  [getting started](getting-started.md#what-the-runtime-wires-for-you).
- **Logging is metadata-only.** Parsley logs through SLF4J and ships no binding; logger
  names are the class names under `io.github.tobyjamesclements.parsley`. INFO covers
  lifecycle: application assembly, task and node initialisation with the source-topic to
  channel mapping, restored hold queues, scope changes, and truncation on a destroyed
  topic. WARN covers a skipped truncation sweep. DEBUG traces each record's hold and
  delivery with its coordinate, dependency clock, and sender claim, which is the level to
  enable when diagnosing why a channel is holding. No level logs payload bytes; every line
  carries coordinates that point at the durable record instead.
