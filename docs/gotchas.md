# Offsets as clocks: what bites when you build causal delivery on Kafka Streams

Kafka orders records within a partition and orders nothing across partitions. If your pipeline has fan-out and fan-in,
an effect can overtake its cause: process A reads an order and emits a shipment, process B reads both topics and sees
the shipment first. Kafka will never tell you this happened.

The fix that suggests itself is to use offsets as a clock. A partition's offsets are monotonic, broker-assigned and
durable, so a message can carry the offsets it causally depends on, and a receiver can hold it back until it has read
past every one of them:

```java
// Carried on every emitted record, in a header.
record Cause(Uuid topicId, int partition, long offset) {
}

// "Every offset at or below this one has been fed to me, or never will be."
Map<Channel, Long> settled;

boolean deliverable(Set<Cause> causes) {
    return causes.stream()
            .filter(c -> received.contains(c.channel()))     // ignore channels I do not read
            .allMatch(c -> c.offset() <= settled.get(c.channel()));
}
```

That is the whole protocol on a whiteboard. This note is about what you find out when you build it on Kafka Streams and
try to make it hold across restarts, rebalances, retention, transactions and operators. Each section names a trap, why
it is a trap, and the shape we ended up with. Most of the traps are the same three Kafka facts showing up at different
layers: an offset is not a message, a topic name is not a channel, and Streams commits things on your behalf that you
would rather it did not.

---

## 1. An offset is not a message

Under `read_committed`, a consumer never sees every offset. Aborted transactions and transaction control markers occupy
offsets that yield no record. Successive records you receive can be at offsets 40 and 43, and 41 and 42 will never
arrive.

That matters because a cause is an offset. If a message says "wait for offset 42 on channel X" and 42 was an aborted
write, the naive receiver waits forever. Worse, the trailing case:
if 42 was the last thing appended to the partition, there is no later record to tell you the gap is a gap.

### What does not work

**Asking the broker for the end offset.** The last stable offset covers records your consumer has *fetched but not yet
handed to your processor*. Treating everything below it as "settled" delivers effects ahead of causes that are sitting
in the consumer's buffer. This is unsafe, not merely imprecise.

**Reading the group's committed offsets.** Streams commits, atomically with each transaction, the consumer position for
each input partition, and that position has moved past aborted batches. It looks like the perfect "read past" report. It
is, but only for a partition that has had a record processed *in the current task lifetime*. After a restart, a
partition with a trailing aborted run has no entry in the task's consumed-offsets map, so nothing ever commits past the
run, and the hold becomes permanent.

**A probe consumer.** A group-less `read_committed` consumer that seeks to the gap and polls once does tell you where
the next real record is, or that the position advanced past an aborted run. It also costs a blocking poll per idle
channel per round, multiplied by task count, on a background thread that every task's liveness now depends on. We had
this for a long time. It worked and it was the single largest source of subtle bugs.

### What we did

Stop trying to observe "will never arrive" from the broker. Make it a **contract on the writer**: a cause must name the
offset of a committed record. Then the receiver needs exactly one rule, which falls out of per-partition ordering:

```java
// On receiving a record at offset o on channel c:
// every offset below o was either fed to me earlier or will never yield a record.
void onReceive(Channel c, long o) {
    settled.merge(c, o, Math::max);
}
```

A gap between two records is settled by the record after it. The record a cause names is one the receiver will
eventually be fed. No timer, no probe, no broker round trip between deliveries. A writer that stamps a marker's offset
or the log-end offset is out of contract, and its receivers hold visibly until the channel's next record, which is the
correct failure mode for a lie.

Writers in your own library satisfy this for free: the only offsets you ever put in a header are ones you received a
record at, or ones you copied from a received header. By induction the whole graph is in contract.

One thing this cannot cover: a cause on a quiet channel *below where the receiver started reading*. If a process starts
at `latest`, or gains a new input topic, an old cause on that topic will never be settled by a receipt. The host has to
hand the engine its **start position** for each partition at initialisation, and everything below it counts as settled.
That is the one report the host owes, and it already has the number.

---

## 2. A topic name is not a channel

Delete a topic and recreate it under the same name. To Kafka Streams it is the same source. To your clock it is a
different log whose offsets mean nothing they meant before. And the consumer group's committed offsets are keyed by
*name*, so they survive the recreation: a restart resumes mid-log on the new topic and silently skips everything below
the old position.

### What we did

Channel identity is **topic ID plus partition**, everywhere: in headers, in ordering state, in the engine. Names are
resolved to IDs once at start and appear only at the edges. Topic IDs are broker-assigned UUIDs and are never reused, so
a cause naming a dead incarnation is recognisably about a channel that no longer exists.

Then bind name to ID in durable state, and refuse to start when a declared name resolves to a different ID than the one
recorded:

```java
Uuid known = orderingState.boundIdentity(topicName);
if (known != null && !known.equals(resolvedNow)) {
    throw failClosed(CHANNEL_IDENTITY_CHANGED,
            "topic '%s' was recreated; committed offsets belong to a dead channel. "
                    + "Reset state and offsets deliberately.", topicName);
}
```

### The sub-traps behind topic IDs

The ID is the right identity, but Kafka makes it hard to ask about.

- **Consumer records carry no topic ID.** The feed is by name. A recreation that completes between two polls, with the
  new log already longer than the old position, is fed to you as if nothing happened. Nothing in the client detects it.
- **Admin offset queries are by name.** Any "describe by ID, then list offsets by name"
  sequence has a race in the middle. Attribute a name-keyed answer to an ID only if the binding held on both sides of
  the query.
- **"Unknown topic" is ambiguous.** Describe-by-ID answers unknown both when the topic is gone and when a
  `DENY Describe` ACL masks it. Treat a single unknown answer as death and a routine ACL change silently prunes live
  causes.
- **One broker's metadata lags.** A describe served from a stale view can report a just-created topic as missing, or
  serve an old name-to-ID binding and convict a live topic as recreated.

The rules that survived every correction: death needs affirmative corroboration by name, over several consistent answers
spaced apart, not one; a denial is never death; an ID whose name you never learned is never confirmed dead, however long
it stays silent. And do the check **once per task initialisation**, on the stream thread, where the answer is applied. A
periodic background check reintroduces every timing window it was meant to close.

For recreation *while a task runs*, we measured what Streams actually does. Deleting a live source topic does not stop
the task. A minute or so later the transactional commit times out, the task is re-created and initialises again, which
is where the check fires. A rebalance that finds the topic missing stops the thread. What remains undetected is a
recreation that lands entirely between two polls with the new log already past the old position. We chose to name that
as an assumption rather than pay a broker round trip per task per second to shrink it.

---

## 3. `auto.offset.reset` is a silent skip

The consumer default resets on an out-of-range offset. In a plain consumer that is a convenience. In a clock it is data
loss: retention passed your committed position, and the consumer quietly resumed at the new log start, treating
discarded messages as settled. Downstream effects are now delivered ahead of causes that no longer exist.

### What we did

`auto.offset.reset=none`, with no per-source reset policy, so an out-of-range fetch kills the task with
`OffsetOutOfRangeException`, which we classify and surface as a named refusal. Then everything the reset policy used to
do silently has to be done explicitly, and each piece has its own trap.

**Initial positions.** With no reset policy, a partition with no committed offset also kills the task, so the runtime
must commit declared initial positions before Streams starts. The obvious tool, `Admin.alterConsumerGroupOffsets`,
succeeds against *any* empty group. A bootstrap that pauses for a while, wakes after a newer instance has run and
stopped, and finds the group empty again, overwrites the newer offsets with stale ones. Kafka has no conditional alter.
The only fenced commit is one made **through group membership**:

```java
try (var member = new KafkaConsumer<byte[], byte[]>(memberProps)) {
    member.subscribe(receivedTopics, new ConsumerRebalanceListener() {
        public void onPartitionsAssigned(Collection<TopicPartition> tps) {
            member.pause(tps);                  // never fetch, never consult a reset policy
            var committed = member.committed(new HashSet<>(tps));
            var toCommit = computeMissingInitialPositions(committed);
            member.commitSync(toCommit);        // generation-fenced: a stale member is rejected
        }
    });
    awaitAssignment(member, deadline);
}
```

Which introduces the next trap: a closed Streams application's members **linger in the group** until their session times
out, and a join from a plain consumer against a group speaking the Streams protocol is refused as a protocol conflict.
So the membership commit runs only when an admin-side read shows offsets missing, and two instances cold-starting
together can still collide. We handle the collision by recognising the protocol-conflict exception in the
uncaught-exception handler and replacing the stream thread instead of shutting the client down.

**Expired offsets.** Offsets expire while a process is stopped. Re-establishing them at the declared initial position is
wrong (a `latest` declaration skips everything retained), and re-establishing at `earliest` is *also* wrong: if
retention advanced past the old position while the process was down, the new log start lies beyond messages this process
never read, and committing it fabricates a read position the host never had. The shape that holds:
resume at the durable **covered position plus one**, and let the fetch refuse if retention has passed it.

**Admin listings skip unstable partitions.** `listConsumerGroupOffsets` with
`requireStable` silently omits any partition with a pending transactional commit. Against a live sibling under EOS, some
partition is nearly always mid-commit, so the listing looks partial, the fast path fails and the start falls into a
90-second group join for no reason. Retry a partial listing briefly before treating it as missing.

**`allow.auto.create.topics` defaults to true.** A bare metadata request from any consumer you build by hand can
recreate a topic you were checking the absence of. For the changelog that holds your ordering state, that means an empty
impostor that passes every prior-state check. Pin it false on every consumer you construct.

---

## 4. Streams commits past what you have delivered

A held-back message has been read. Streams will commit the read position past it at the next commit. If the process
crashes, that message is never re-fed. Your hold-back buffer is therefore state that must survive with the same
atomicity as the offsets.

### What we did

Held bodies live in a **changelogged state store**, flushed before the transaction can commit, so restore rebuilds the
buffer exactly. The traps here are all about not trusting defaults and not trusting the changelog read:

- **Request logging and compaction explicitly.** Both are Streams defaults today. A store that silently restored empty
  after a defaults change would collapse the whole guarantee.
- **Enable the store cache.** A received record merges its whole frontier into ordering state, one put per channel whose
  position advanced. Uncached, that is hundreds of RocksDB writes and changelog records per input record, and a ceiling
  near a thousand records a second per task.
- **Task width is rigid.** The changelog gets one partition per task, the task count follows the widest input topic, and
  Kafka cannot shrink a topic. Any change to the induced width must be refused at start with a real diagnosis. Left
  alone, Streams reports healthy for forty seconds and then dies inside internal-topic validation, advising a reset tool
  that would destroy the state you were protecting.
- **Stranded holds.** Removing an input topic can shrink the task set so the task holding messages on that topic is
  never instantiated, and no per-task check ever runs. The runtime has to read the ordering changelog end to end at
  start and refuse if any live hold names a channel outside the new declaration.
- **Read the changelog to the uncommitted end.** A task that moved between threads can leave a predecessor's transaction
  open below records the successor committed. Bounding the scan by the last stable offset silently truncates it. Read to
  the `read_uncommitted` end and tolerate empty polls up to a stall deadline longer than the transaction timeout.
- **Keep presence markers, not bodies.** Holding every held blob in memory during that scan fails a restart with an
  out-of-memory error exactly when the backlog is largest.
- **Lost state with surviving offsets.** An operator deletes the changelog and keeps the group. The start looks like a
  first start, Streams recreates the changelog empty, and the process resumes mid-log with an empty clock. Streams
  overwrites offset metadata with its own stamp on every commit, which means you cannot store anything there, but it
  also means a committed offset *without your bootstrap's own stamp* proves a prior Streams execution happened and its
  state should exist. Refuse the start when it does not. Judge this per changelog partition, not per topic.

---

## 5. Streams' escape hatches are all paths to skipping a message

Every "continue on error" facility in Streams converts a failure into delivering past the failed message with the read
position advanced. That is precisely the thing a clock cannot survive. The list grows as you audit:

```java
static final Set<String> FORBIDDEN_SUFFIXES = Set.of(
        "processing.guarantee", "isolation.level", "auto.offset.reset",
        "enable.auto.commit", "group.id", "transactional.id",
        "deserialization.exception.handler",
        "production.exception.handler",
        "processing.exception.handler",
        "interceptor.classes",           // a producer interceptor can strip your header
        "default.timestamp.extractor",   // LogAndSkip is a documented drop
        "group.protocol", "group.remote.assignor",  // different fencing semantics
        "bootstrap.servers"              // under a consumer prefix, points at another cluster
);
// Matched as suffixes, so refused under any prefix: main.consumer., restore.consumer., producer., ...
```

Related, less obvious:

- **Do not forward records from `Processor.init`.** Nothing in Streams exercises that path. Deliver only from `process`
  and punctuation.
- **Emission timestamps inherit along the chain.** The Streams convention gives an emission the delivered record's
  timestamp, so after k hops every message carries the origin's time. Time-based retention on an intermediate topic can
  discard an emission on arrival. Let the handler choose a timestamp derived from delivered data. Never from a clock, or
  replays diverge.
- **A handler that throws must stop the process.** Restart re-delivers the same message and it throws again. That is
  correct. Dead-lettering is the application's decision, made inside the handler, never the runtime's.

---

## 6. Pruning is where causal order actually breaks

Every cause you carry costs bytes on every message forever, so you want to drop causes that can no longer matter. Each
rule for doing so that looked obviously safe was not.

**Prune by log start.** If a cause's offset is below the channel's earliest retained offset, surely nothing can be
waiting on it. Except a process that received the message and is *still holding it*: its senders prune, their later
sends stop mentioning the message, and the holder delivers those sends past the held message. No local failure anywhere.
The holder has no way to know which later arrivals depended on what its senders forgot.

**Prune on channel death.** Same shape. A deleted topic with messages still held from it lets senders forget causes the
holder still owes.

**Fail closed per channel.** After an undecodable header, you do not know what causes that message carried, so any
further send from this process may under-express. A quarantine scoped to one channel launders a causal inversion through
"just that channel".

### What we did

- Prune a cause only when its **channel is dead**, never for its age. Retention discarding a held message is not a
  failure: the hold lives in the changelog and delivers from there, and its senders keep expressing it. Retention need
  cover the longest stop and lag, not hold-back time.
- A dead channel with messages still held from it **fails the process closed**. With nothing held, it settles to the end
  and its causes are pruned.
- Fail closed means the **whole process stops**, and stays stopped until an operator acts.
- A channel **joining** a process later must not deliver causes behind effects the process already delivered. Keep a
  separate, persisted "delivered causal past" and clamp the joining channel's start above it. The send frontier is the
  wrong thing to clamp with, because it includes causes of messages still held.

---

## 7. Metadata size hits an uncompressed wall

The frontier a message carries approaches the sum of partition counts across the transitive upstream closure. Producer
size limits are judged on uncompressed bytes, so batch compression never moves the ceiling. Cross-channel compression is
unsound: dropping cause (c₁, p₁) as "covered by" (c₂, p₂) requires every reader of c₁ to also read c₂, which partial
consumption breaks.

The only honest controls are a **byte budget** that fails closed with a diagnosis before the substrate's limit fails you
without one, enforced on receipt, on merge and on emission, and a **compact grammar**: group entries by topic ID so the
16-byte UUID is written once per topic, varint the small structural fields, and keep positions fixed-width so encoded
size is a function of topology rather than of how long the logs have been growing.

---

## 8. Testing: the engine cannot judge itself

- **Keep the oracle outside.** Maintain a happened-before relation from ground truth in the test harness, never from
  what the engine reports. An oracle that learns of a receipt only when the engine accepts it cannot see a silent drop.
  Ours went 380 tests green with an engine that discarded every third message.
- **Sabotage, not syntax.** Build the engine with switchable violations, one per safety property, and assert the suite
  catches each with a measured margin over seeded random runs. Mutation tools score boilerplate; sabotage modes score
  the guarantee.
- **"Verified in the sources" is per version and per case.** The committed-offsets claim in section 1 was true of the
  Streams source for the same-lifetime case the integration test exercised, and false after a restart. Re-verify every
  substrate claim against a real broker, on every upgrade.
- **A single embedded broker hides metadata lag.** A describe served from a lagging view refuses a topic created a
  moment ago. The race is the runtime's, not the test's; the fix is corroboration in the runtime, not a sleep in the
  test.
- **Make the decision pure.** Deliverability should be a static function of the message, the ordering state and nothing
  else: no clock, no network, no host type. Enforce it by scanning the package. Everything that touches time or the
  broker lives outside that function and only ever feeds it lower bounds.

---

## The short version

Using offsets as a clock is sound. Kafka Streams is a workable host for it. The work is almost entirely in refusing to
trust the parts of Kafka that are designed to be forgiving:
reset policies, name-keyed offsets, continue-on-error handlers, admin calls that are not fenced, listings that skip what
they cannot answer, and defaults that create topics on request. Every one of those is a convenience in an ordinary
consumer and a silent causal inversion in a clock. Where the guarantee cannot be upheld, stop, name why, and stay
stopped until a person decides.
