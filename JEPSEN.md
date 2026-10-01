# Jepsen findings

What the Jepsen test in [parsley-jepsen](https://github.com/tobyjamesclements/parsley-jepsen)
found when it first met a real cluster, on 2026-09-30. The test's design, its checker and
how to run it are in that repository's README; this page is the record of what the runs
showed about Parsley, about the checker, and about the environment, so that nothing here
has to be rediscovered. Criteria are cited as in `SPEC.md`.

## What was run

- Apache Kafka 4.3.1, KRaft, one combined controller-plus-broker per node, three Debian
  bookworm nodes from `jepsen-io/jepsen` at v0.3.9 (`docker/`, which later releases
  dropped), on one 8 GB Docker VM on an arm64 Mac. Brokers and harness instances at 512 MB
  heaps. Topics of three partitions, replication three, `min.insync.replicas` two.
- The harness app from this repository's test tree (`JepsenHarness`, built by
  `./mvnw -Pjepsen-harness -DskipTests package`), one instance per node, all four processes
  of `JepsenTopology` (`splitter`, `joiner`, `cycler`, `selfer`) under the prefix `jepsen`.
- Runs of two minutes to an hour, external sends at two to twenty per second. Every run is judged
  twice over one export: by the Clojure checker (`checker.clj`) and by this repository's
  replay (`JepsenExportOracle`, `java -jar <harness> check`). The two never disagreed on a
  verdict once their rules were made the same.
- Forty-four runs in all. The first clean run judged 7318 trace entries and 7318 committed
  records valid in both checkers. The manufactured inversion (`--calibrate inversion`, an
  external producer that stamps less than it knows) was flagged by both: Safety 1 at
  delivery time and over the delivered pair, and Structural 15 on every downstream send
  that could not express the hidden cause.

## What Parsley did under each fault

Each fault ran in a run of its own, at most once per process, and then all of them
together in a ten-minute run. The table is the one in parsley-jepsen's README; the right
column is what happened.

| Fault | Expected | Observed |
|---|---|---|
| Delete records past a lagging task's committed position (`src`, instances down, external sends continuing) | `POSITIONS_DISCARDED_UNREAD` | Came, at the splitter. Nothing delivered past the gap. |
| Retention discards a held message's copy (records deleted up to the committed position, live; then retention itself, a minute's, across a hold) | No refusal; delivered from the ordering changelog in order | No refusal. With retention on its own clock the held record's copy was gone from the topic (log start past it) while the hold stood, and it was delivered in order from the changelog once the stamp settled. |
| Delete a received topic while messages are held from it (`self`, live, then a restart) | `CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES` | Did not come at first; see below. Came at the next start once Parsley made the diagnosis there, naming the topic and the held channels. |
| Delete and recreate a received topic while the process is down (`c`) | `CHANNEL_IDENTITY_CHANGED` at the next start | Came, at the cycler's start: "topics [c] now resolve to different identities than this process's state was built against". |
| Reset the group's offsets backwards while the process is down (one to five back) | Re-fed records dropped; no duplicate | No refusal, no duplicate delivery, three times. |
| Delete the ordering changelog, keeping the group's offsets (local state wiped too) | `ORDERING_STATE_LOST` | Came, at the splitter and, in the mixed run, the joiner. |
| Add partitions to the widest received topic (`a`, three to four), then restart | `TASK_WIDTH_CHANGED` | Came, at the joiner. |
| Restart with a declaration dropping a topic that holds messages (`self`) | `CHANNEL_REMOVED_WITH_HELD_MESSAGES` | Came, at the selfer. |
| Malformed `parsley.causes` header (version byte 99) | `UNDECODABLE_METADATA` | Came, at the splitter; nothing delivered past it. |
| Stamp naming the log-end offset (a tenth of all sends) | A hold until the channel's next record; no refusal | No refusal. The hold was settled by a transaction marker as well as by a record; see below. |
| Partition, broker kill, pause, clock skew | No refusal | No refusal under partitions (one node, a majority, a ring), broker kills and SIGSTOP pauses (Kafka 3.7.0), instance kills and pauses lasting about a minute (past the transaction timeout), and clock bumps and strobes. |

The mixed run (partitions, instance kills and pauses, truncation, recreation, changelog
deletion and the narrower declaration, ten minutes) judged valid with all four refusals the
table expects, after three attempts that found faults landing inside a partition; those are
under [the nemesis](#about-the-nemesis). A twenty-minute run on Kafka 3.7.0 at five sends a
second, under partitions, broker and instance kills and pauses, retention and ten offset
resets (56,352 trace entries, 47,554 records), judged valid in both checkers in 87 seconds.
Then an hour on each version with the same faults: 4.3.1 valid over 48,905 entries; 3.7.0
with one violation the nemesis made (below). Kafka 3.7.0, the floor the spec names,
behaved as 4.3.1 did in every run on it. Past the hour, four runs asked different
questions of the same cluster, and all four judged valid in both checkers: retention on
its own clock across a hold (above); a fault every fifteen seconds for a quarter of an
hour, partitions, broker kills and pauses and instance kills and pauses overlapping (184
faults, 14,212 entries); the runbook's reset after each refusal, with the process's next
lifetime judged on its own (below); and twenty sends a second for ten minutes (133,171
entries, 1,063 held out-of-contract stamps). With nothing refused or discarded a run's
trace entries equal its committed records, since every record is received by exactly one
process and traced once; retention and refusals are what separate the two counts.

### Under unclean leader election, Kafka's loss is named as Kafka's

The labelled run (`--unclean-leader-election`, broker kills, pauses and partitions, ten
minutes) is invalid, and the checkers say why. An unclean election truncated the joiner's
ordering changelog, its group's committed offsets and the trace behind what it had
committed, and the joiner resumed from the older state: its expressed frontier on one
channel fell from 309 back to 232 along its own trace, it re-delivered records it had
delivered before, and every later send under-expressed. Parsley cannot see this: the
changelog's head is intact and the offsets agree with it, so nothing is missing from where
it looks. Both checkers now report a frontier that falls along a task's trace as **Host
obligation 5** (resumed from a state older than the one committed, which the host lost),
118 times here, beside the 921 Structural 15 and 5 Liveness consequences, so the verdict
attributes the run to the substrate rather than to the implementation. A channel that no
longer exists is the one exception, since its causes may be discarded (Structural 13); the
simulator's recreations exercise it. The rule runs in every run, and no clean run has
tripped it.

### A fault the nemesis did not mean

The one violation in the hour on 3.7.0 was `POSITIONS_DISCARDED_UNREAD` at the splitter
with no fault to justify it. A `reset-offsets` had timed out client-side inside a
partition, but the broker applied the alter later; in between, a `discard-held-copy` read
the still-unreset committed position and deleted the records before it, so when the late
reset landed the group's position was below the log start, and the splitter refused
exactly as Safety 8 says. Two lessons for anyone operating this: an admin request that
times out may still happen, and a position read from one broker during a partition is one
broker's opinion. The nemesis now reads a group's position as the lowest of every broker's
view, a log start as the highest, and reads an alter back until the group shows it.

Across every run: no Safety 1, 2 or 3 violation, no Structural 14 or 15 violation, no
duplicate commit of an effect (Host obligation 6), no delivery from a channel outside the
declaration, and no refusal that an injected fault did not justify (Operational 1 and 6).

### Deleting a received topic never reaches the refusal

`docs/failing-closed.md` says `CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES` is raised "at task
initialisation, by the identity check that finds the topic gone... while the task still
holds messages from it". On the cluster the host gets there first. With a record held on
`self` (its stamp named a position a million past `d`'s log end) and `self` then deleted
while the selfer ran, every instance's selfer went `REBALANCING` and then `STOPPED` with
Kafka Streams' own error, "One or more source topics were missing during rebalance", and
no refusal: the Streams assignor will not assign tasks for a topology whose source topic is
missing, so no task is ever initialised. A restart afterwards refused to start at all, with
`IllegalStateException: declared topics could not be resolved`, which `docs/runbooks.md`
classes as a prerequisite failure, not a refusal.

So the row's refusal was unreachable through Kafka Streams 4.3.1 by plain deletion. The
process did stop, and stayed stopped, which is the fail-closed outcome; but the status said
nothing Parsley-shaped about why, the runbook for the refusal did not apply, and the test
reported a missing expected refusal.

The resolution kept the refusal and moved the diagnosis to where the host leaves room for
it. At start, where a received topic no longer resolves, `StreamsRuntime` now reads the
process's ordering state before giving up: the state knows the identity the name was bound
to and which channels hold messages, so a missing received topic with held messages from
it refuses `CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES`, naming the topic and the channels,
and a missing topic with nothing held stays the prerequisite failure it was. The
`delete-topic` fault restarts the instances after the deletion, and the run judges valid
with the refusal where the table says. `docs/failing-closed.md` and the runbook say that a
live deletion is reported by the host first and by Parsley at the next start.

### A start that refuses one process refused them all

`Parsley.start(config, p1, p2, p3, p4)` resolves every process before any runs, and a
refusal for one (the cycler's `CHANNEL_IDENTITY_CHANGED`) left none running: the harness
served every process as `STOPPED`, three of them with no reason, and the run could not
quiesce. That is the documented contract of one `start` call, not a bug, but an application
that declares several processes in one call should know that one process's state is enough
to keep the others down. The harness now starts each process with its own call, as separate
applications would.

### A held out-of-contract position travels, and a marker settles it

An external stamp naming `a-0`'s log end (113) was held, as it should be. Two things
followed that the checkers had not allowed for:

- Position 113 was never assigned to a record. After the splitter refused, the aborted
  transactions' markers took the offsets at and past the end, so the hold was settled by
  the channel moving past 113 with no message there. `Liveness 3` says exactly this ("the
  positions between messages that yield none... are settled by the receipt of the next
  message"), and the engine did it.
- While held, the stamp's position was in the joiner's frontier and every downstream send
  expressed `a-0@113`, an unassigned position. `Structural 12` allows it ("it MAY express a
  position it learned from the metadata of a message it received"), and that clause is load
  bearing: a receiver cannot know whether a position in received metadata is assigned.

Both checkers flagged this as Structural 12 until they were taught the clause and given
each partition's log end (see below).

### A broker down at start-up takes a process down for good, unless the application retries

`Parsley.start` throws `IllegalStateException` when the cluster cannot be queried in time
("committed read positions could not be listed", "declared topics could not be
resolved"). With a broker killed forty seconds into a run, while the joiner was starting,
its start timed out and the harness, which started each process once, served the joiner
as stopped for the whole run; the other three ran. The runbooks class this as a
prerequisite failure to retry, and the harness now does, every five seconds, until the
process runs or refuses. An application should expect the same of itself: a start is not a
refusal, and a broker outage at the wrong second is enough to hit it.

### Kafka Streams keeps its group membership on close

A graceful stop (SIGTERM, the harness's shutdown hook closing every `Parsley`) leaves the
group `Stable` with its members for the full session timeout, 45 seconds by default, since
Streams does not send `LeaveGroup` on close. A kill does the same. Altering the group's
offsets needs the group empty, so the runbook step "reset the group offsets" after a stop
waits that long; a test that tries sooner fails with the group still stable.

### After the runbook's reset, the next lifetime runs clean

A refusal is terminal for the process, and `docs/runbooks.md` says what an operator does
next: stop every instance, delete the group and its ordering changelog, wipe local state,
recreate a deleted received topic, and start again from an initial position. The nemesis
now does exactly that after each refusal-class fault (`--reset-after-refusal`), and the
harness runs the process on as its next lifetime, labelled `name#2` in its trace and in
the uids it forwards (`--incarnation`), from the earliest position or, after an
undecodable header, from the latest (`--initial-position`). Two runs covered six of the
seven refusals: `delete-topic` and `corrupt` (the selfer's reset recreated `self`; the
splitter's started past the malformed record), and `truncate`, `delete-changelog`,
`recreate-topic` and `restart-dropping`, each followed by its reset. Every lifetime
bootstrapped, committed its start positions, ran to quiescence and judged valid in both
checkers as a process of its own, from the positions its bootstrap committed: no causal
inversion, no duplicate, no under-expression, in a lifetime that re-delivers what the one
before it delivered and sends the effects again as new messages. The one thing the
lifetime's start positions showed is worth knowing: a `latest` start committed the
partitions' ends at bootstrap (107, 85, 86 on `src`) and an `earliest` one zero, except
where the earlier lifetime's `truncate` had moved the log start (25), which is where
`EARLIEST` begins.

## What the checkers got wrong, and the rules that fixed them

These were false positives on a correct engine, and were not papered over: each fix is a
rule in both checkers with the reason in the code, and the simulator calibration set
(three seeds per sabotage mode, regenerated) still catches every mode.

1. **Structural 12 was judged against the highest committed record.** Transaction markers
   and aborted records take offsets too, so the last assigned position is the log end less
   one, which `JepsenExport.TopicInfo` now carries per partition (`log-end`; the simulator
   counts every slot as its own `world.lastAssigned` does, the cluster dump reads the high
   watermark). And a position learned from received metadata is excused whether or not it
   is assigned, which is the clause above.
2. **Evidence that retention or deletion removed was judged against.** The expression bound
   (everything a sender could have seen expressed) was built from the records in its
   receipt spans; where retention had discarded some, or the received topic had been
   deleted and recreated, the bound was too low and every later send was over-expression.
   Where a task's receipt spans reach records the export does not hold, the bound is
   unknown and the over-expression and Structural 12 checks are not made. The simulator's
   export keeps every record its retention discarded, so its calibration is unaffected.
3. **A stale coordinator is not an observation.** Under a partition the control node's admin
   client asked a group coordinator that the partition had cut off, and it answered from
   its cache with positions lower than an earlier observation's. The checker's receipt
   bounds assume an observation taken after a step is current, so one joiner step was
   flagged for expressing what it had legitimately received. The client now takes every
   observation (the trace ends before and after, every group's positions) as the freshest
   of every broker's view, their maximum, since these only grow; and the checker sets aside
   any cluster observation that reports a position or a trace end below an earlier one,
   unless a `reset-offsets` fault between them says what it rewound.
4. **Liveness is a judgement at quiescence.** A run whose final phase never reached zero lag
   on every running process is not valid whatever the trace says; the result carries the
   lag it ended with.
5. **The replay was quadratic, and is not now.** Both checkers kept every record's true
   causes as an explicit set, as the simulator's `Oracle` does: 22 seconds for a two-minute
   run, 318 for a five-minute one, and the Java replay exhausted a 3 GB heap past about
   20,000 records. A causal past is now a frontier, the greatest position per channel,
   standing for every position up to it; under FIFO delivery (Safety 3, judged on its own)
   that is the past exactly, since a position is delivered exactly when every record before
   it on its channel is, so "every cause delivered" is "the greatest on each channel
   delivered" and "every cause expressed" is "the greatest expressed". Receipt spans are
   merged per observation prefix and what received records name is kept per channel prefix,
   both found by binary search. Over every stored export and the calibration set the
   verdicts are the ones the sets gave; the five-minute run's 17,404 entries judge in 5
   seconds in Clojure and 1 in Java. `JepsenExportOracle` no longer replays through
   `Oracle`, whose sets are the simulator's business; it keeps the same rules in its own
   frontier model, and the calibration test holds it to the simulator oracle's verdicts.
6. **A lifetime the reset attached to a recreated topic is not a task that went on.** The
   cluster form of the Assumption 2 rule flagged, by topic name, every delivery after a
   recreation by a process that received the topic, which caught the cycler's second
   lifetime doing what the runbook prescribes. The rule now falls on the tasks that
   received the dead incarnation; a task attached to the new one is judged like any other.

## About the nemesis

What the fault injection learned, for whoever extends it.

- A fault that lands inside a network partition may half-happen. `deleteTopics` timed out
  with the deletion under way, so `c` was gone and never recreated, and the cycler and the
  joiner could not start; a manufactured hold's send timed out at thirty seconds though the
  record got through, so the fault gave up before restarting anything while the record was
  held. Deletion and recreation now retry until the name resolves to a new incarnation, the
  hold's producer waits out a partition, and a fault says in its result whether it applied,
  so the checker expects a refusal only of a fault that happened.
- Scheduling: in Jepsen's pure generators a `sleep` op holds only the thread that performs
  it, so "sleep, then op" runs the op at once on another thread. The malformed record was
  the first record on its partition, and the first fault of a run landed while instances
  were still starting. Faults are now scheduled by the test's clock (`workload/after`).
- Jepsen's file-corruption nemesis already answers to `:truncate`; record truncation is
  `:truncate-records` on the wire.
- A refusal is terminal, so a run schedules each refusal-class fault once, at a process no
  other fault has; the plan is in `nemesis/refusal-targets`. With `--reset-after-refusal`
  the runbook's reset follows each, and three things about observing the lifetime it
  starts were learned the hard way: a lifetime that attached to a recreated topic commits
  under the new topic id, so the group's positions are keyed by the ids the reset found,
  where the run's start ids served until then; a process whose declaration dropped a topic
  commits nothing on it, so the reset does not wait for that; and what is observed of a
  process while its reset is under way (a deleted group, a bootstrap committing under the
  old name) belongs to neither lifetime and is set aside.

## About the environment

- A KRaft broker that cannot register with the controller quorum within a minute exits.
  On fresh nodes the brokers started minutes apart while each downloaded Kafka from
  `archive.apache.org`, and none survived; setup now meets at a barrier before any broker
  starts. The download is slow (ten minutes) only the first time.
- The harness truncates its log file at start; the DB adapter keeps the previous instance's
  log in a `.history` file, which Jepsen collects.
- Memory: three brokers, three instances (four Kafka Streams applications each, with
  RocksDB), the control JVM and the replay share 8 GB. Nothing else should run on the VM
  during a test. An instance at 512 MB ran out of heap while a broker was down, as every
  producer's buffer filled; 768 MB holds.
- The brokers the final generator restarts are still electing leaders when the final phase
  begins, so the quiescence wait and the dump retry rather than fail on the first timeout.
- Docker nodes share the VM's clock, so the clock nemesis is a jump for the whole cluster,
  including the control node, not skew between nodes; the VM was minutes off afterwards
  and wanted `ntpdate`.
- `pkill -f` with the pattern's first character bracketed (`[p]arsley-jepsen-harness`) is
  how processes are found, since every signal goes through a shell whose own command line
  would otherwise match.

## Not yet done

- Clock skew as skew; more than three nodes.
- Anything longer than an hour. The refusal-class faults with their resets inside an
  hour-long run, and `add-partitions` followed by its reset.

