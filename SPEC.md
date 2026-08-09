# Specification for Parsley

## Introduction

Parsley provides causal delivery order for Kafka Streams processors. By default, Kafka Streams processors may only rely
on a total order per topic-partition. There is no guaranteed delivery order between topic-partitions. The aim of Parsley
is to provide this guarantee using the existing Kafka Streams libraries and other related Kafka libraries.

This document is the complete specification. An implementation satisfying every requirement addressed to it is correct
regardless of the implementation's details, provided the host meets the obligations placed on it under **Host
obligations**. Anything this document does not specify is a decision for the implementer, who MUST record each such
decision, and the alternatives rejected, in `DECISIONS.md`.

## Conventions

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT
RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described
in [BCP 14](https://datatracker.ietf.org/doc/bcp14).

## Terminology

The following terms are used throughout this specification and are defined here for clarity:

* **Message**: a unit of data sent on a channel, comprising a key, a value, and metadata.
* **Channel**: a totally ordered sequence of messages. Any number of processes MAY send to a channel, and any number MAY
  receive from it, including a process that does both.
* **Position**: a message's place in the order of its channel, drawn from a totally ordered set and strictly increasing
  along that order. Positions need not be consecutive, and a position may exist that never yields a message a process
  receives.
* **Earliest retained position**: the lowest position on a channel the substrate can still return. Messages below it
  have been discarded by the substrate.
* **Process**: a unit of computation that receives, delivers and sends messages on one or more channels.
* **Execution**: the span of a process's lifetime from one full initialisation to the next stop. A process's lifetime
  is a sequence of executions.
* **Received-channel set**: the channels a process is declared to receive from, whether or not it has yet received on
  them.
* **Send**: the act of a process writing a message to a channel.
* **Receive**: the act of a process reading a message from a channel.
* **Deliver**: the handoff of a received message to application logic. A message is delivered at most once per process,
  at a definite point in that process's execution.
* **Step**: the host's unit of atomic commitment — one or more consecutive deliveries at a process, together with the
  state they mutate, the messages they send, and the read positions they consume.
* **Happened-before**: the smallest relation over events such that A happened-before B if A precedes B within the same
  process, or A is the send of a message and B its receipt, closed under transitivity.
* **Cause**: message A is a cause of message B when some delivery of A, at any process, happened-before B's send.
  Because happened-before passes through receipt, the causes of a message a process has received but not yet delivered
  are already causes of that process's subsequent sends.
* **Concurrent**: two messages are concurrent when neither is a cause of the other.
* **Causal delivery**: delivery such that if A is a cause of B, any process delivering both delivers A before B.
* **Deliverable**: a received message is deliverable at a process when delivering it there would not violate causal
  delivery.
* **Express**: metadata expresses a cause when it carries a pair naming the cause's channel and a position at or above
  the cause's.
* **Application state**: the state application logic reads and writes.
* **Ordering state**: the state an implementation holds in order to decide deliverability and to express causes.
* **Fail closed**: to stop delivering — at minimum on the affected channel — rather than proceed with a weakened
  guarantee. A failure is never converted into a delivery, or into delivering past the failed message.

## Acceptance Criteria

### Safety

1. An implementation MUST provide causal delivery. If message A is a cause of message B, any process that delivers both
   MUST deliver A before B. This binds across restarts: a process that resumes after failure is the same process, and
   its delivery order is judged over its whole lifetime rather than per execution.
2. A process MUST NOT deliver the same message more than once. A delivery rolled back by an aborted step has not
   occurred, and delivering that message subsequently is not a duplicate.
3. Where a message is not yet deliverable, an implementation MUST hold it, and every message received later on the same
   channel, rather than delivering past it.
4. An implementation MUST carry all metadata this specification requires as metadata attached to the message, separate
   from the message's key and value. A message's key and value MUST be exactly those the application sent: no wrapping,
   prefixing or extending, and no value required where the application sent none.
5. A reader with no knowledge of this specification MUST be able to decode any message an implementation sends, using
   the application's own codecs alone. In particular, an application MUST be able to use Avro for message keys and
   values, including the Confluent Schema Registry wire format.
6. A message carrying no causal metadata MUST be treated as having no causes, and MUST be deliverable immediately. An
   implementation MUST NOT require that every message it receives was sent by an implementation of this specification.
7. Metadata that is present but cannot be decoded MUST fail closed. It MUST NOT be treated as absent, and the message
   MUST NOT be delivered on the basis that it carries no causes.
8. Where a process's read position on a channel is, or resumes, below the channel's earliest retained position, an
   implementation MUST fail closed rather than treat the discarded positions as positions that never carried messages.
9. A channel ceasing to exist does not discharge causal delivery for messages already received from it. Where a
   process retains a received-but-undelivered message from a channel that no longer exists, an implementation MUST
   NOT treat the channel's disappearance as removing that message's place in causal order; where it cannot preserve
   that order it MUST fail closed rather than deliver past the message. (Assumption 17 states what an implementation
   may assume of channel deletion; this criterion binds where the implementation detects that assumption breached.)

### Liveness

Every criterion in this section holds on the premise that a process is eventually scheduled and continues to make
progress, that a process which stops is eventually restarted, that the channels in its received-channel set remain
readable, and
that the host continues to meet its obligations — in particular that it eventually feeds every message its channels
hold, and that its read position eventually advances and is reported as Host obligation 2 requires. A message held
under Safety 7, and every message held behind it under Safety 3, is exempt while its metadata remains undecodable.
Stated without these premises each criterion is false, and no implementation can be held to it.

1. Every message a process receives MUST eventually be delivered at that process.
2. Sending MUST NOT block on the deliverability of the message sent. Only delivery waits.
3. Where a cause names a position that will never yield a message, an implementation MUST still eventually deliver.
   Detecting this MUST NOT rely on elapsed time.
4. Where a cause names a channel outside a process's received-channel set, that process MUST still eventually deliver.
5. A message received but not yet delivered MUST still be delivered after the receiving process restarts, where its
   channel remains in the received-channel set.

### 2-safety

1. Application use of application state MUST NOT affect whether any safety or liveness criterion holds. An
   implementation whose correctness depends on how an application uses application state has not met this requirement.
2. A restart that does not change a process's received-channel set MUST NOT be observable in what the process delivers
   or in what order. It MAY be observable as a pause.

### Fault model

1. An implementation MUST NOT assume that successive messages on a channel occupy consecutive positions.
2. An implementation MUST NOT rely on any bound on process execution speed, scheduling delay, or clock progress. Every
   safety criterion MUST hold under process pauses of arbitrary duration — including a pause that outlasts any timeout,
   lease or session the host maintains, and a pause that ends with the process resuming and acting on state that is no
   longer current.
3. A process MUST be able to restart at any point, including partway through handling a message.
4. An implementation MUST NOT assume that every position on a channel will yield a message it receives. Positions may be
   occupied by data the substrate never returns.

### Structural

1. An implementation MUST decide delivery locally, from messages a process has received and state it holds — no
   consensus, no coordinator, no global clock, and no exchange between processes other than the channels themselves.
2. An implementation MUST support any arrangement of processes and channels, including cycles and channels from a
   process to itself.
3. The seam through which an implementation invokes application logic MUST pass only the delivered message and the
   process's application state, and MUST accept the logic's effects only through its returned value — the messages to
   send and the application state to persist — which the implementation applies within the step. It MUST hand the
   logic no other capability.
4. The public API MUST be statically typed over the application's message key and value types, per channel.
5. The representation of the metadata a message carries MUST be documented and stable, MUST NOT depend on any
   implementation's internals, and MUST be distinguishable, by construction, from metadata applications or other
   systems attach to their messages.
6. Where an implementation uses a term of art from the distributed systems literature, it MUST name the thing that term
   denotes.
7. The decision whether a message is deliverable MUST be a pure function of the message, the ordering state the
   process holds, and the criteria in this document. It MUST NOT read wall-clock time, measure elapsed time,
   communicate with anything outside the process, or depend on anything else outside its arguments. Nor may it derive
   deliverability from a message's application-set timestamp: that is data an application controls, not evidence about
   order. An implementation MUST expose that decision as a separately callable unit — the same unit it invokes for
   every delivery it performs — testable in isolation without a host or a running process.
8. An implementation MUST permit application code to read and write its application state, including through state
   facilities the host provides, and MUST NOT require exclusive ownership of them. An implementation MUST keep its
   ordering state where application state cannot alter it.
9. An implementation's public API MUST NOT expose any operation whose use, as documented, can cause a safety criterion
   to be violated. Where an operation would make that possible, it MUST be absent rather than documented as unsafe.
10. An implementation MUST NOT provide timers, scheduled callbacks, or any means of causing a message to be delivered
    other than by receiving it from a channel. Periodic work is performed by an application sending messages from
    outside the implementation.
11. A message's metadata MUST express its causes solely as (channel, position) pairs. No other form of dependency is
    permitted, and metadata MUST NOT carry an identity for the sending process.
12. A process MUST NOT express a dependency on a position that has not yet been assigned. It MAY express a position it
    learned from the metadata of a message it received, and it MAY treat every position below its first receipt on a
    channel as already satisfied.
13. The metadata a message carries MUST NOT grow without bound as an application runs. An implementation MUST have a
    means of discarding causes that can no longer matter, and MUST NOT discard any other cause. A cause can no longer
    matter exactly when its position is below its channel's earliest retained position, or when its channel no longer
    exists. Replacing two pairs on one channel by the single pair with the greater position is compression, not
    discarding.
14. A process MUST be able to send to a channel it also receives from. A message so sent MUST NOT depend on itself or on
    any position at or above its own, however its causes are computed.
15. A message's metadata MUST express every cause of that message whose position is at or above the cause's channel's
    earliest retained position — including causes known only from the metadata of messages received and not yet
    delivered.
16. A process's received-channel set MAY change between executions. Causal past a process has already delivered MUST
    NOT be dropped when a channel leaves that set, and MUST NOT be re-entered when a channel joins it. An
    implementation MUST refuse an execution whose declaration removes a channel on which received messages remain
    undelivered.
17. An implementation MUST provide a public API through which an application declares its processes, the channels each
    receives from, and the channels each sends to.
18. That API MUST expose the generality Structural 2 requires of the protocol: several processes, each receiving from
    and sending to any number of channels, each channel carrying its own key and value types, and each process holding
    any number of separate stores of its own application state.
19. Every message application logic emits MUST be sent to the channel the application named, atomically with the step
    whose delivery produced it; a step that cannot send does not commit. An emission naming a channel outside the
    declared send set MUST fail the step: it MUST NOT be dropped, and MUST NOT be sent without the metadata this
    document requires.
20. Every choice this document leaves open that an implementation takes MUST be recorded in `DECISIONS.md`, with the
    alternatives rejected. Relying on an assumption is such a choice.

### Substrate and toolchain

1. Messages MUST be carried by Apache Kafka, brokers version 3.7.0 or newer.
2. The host MUST be Kafka Streams.
3. Processing MUST be exactly-once: `processing.guarantee=exactly_once_v2` and `isolation.level=read_committed`, neither
   overridable by an application.
4. The implementation language MUST be Java 21.
5. The build MUST be Maven.

### Host obligations

These bind the host rather than the implementation. An implementation cannot enforce them, but where it detects one has
been breached it MUST fail closed rather than degrade.

1. Within one execution of a process, the host MUST feed it the messages of each channel in increasing position order,
   and MUST NOT withhold a message indefinitely.
2. The host MUST report a process's read position on a channel. A reported position asserts that every position below
   it has either been fed to the process as a message or will never arrive as one; the host MUST NOT report a position
   covering a message it has received but not yet fed. The read position MUST eventually advance past positions that
   never arrive as messages — including a trailing run with nothing after it — and each advance MUST be reported.
3. The host MUST commit, atomically, the state a step mutates, the messages it sends, and the read positions it
   consumed.
4. The host MUST restart a process through its full initialisation, not resume it in place.
5. After a restart, the host MUST resume a process from its most recent committed step: the state the process observes
   at initialisation MUST be the state as of that step, and the host MUST feed again every message whose consumed read
   position that step did not commit.
6. The host MUST NOT commit a step begun by an execution it has superseded by restarting the process elsewhere.

### Operational

These are desired characteristics rather than conditions of correctness. An implementation SHOULD satisfy each;
where it deviates, the deviation and its rationale MUST be recorded in `DECISIONS.md` (Structural 20).

1. An implementation SHOULD expose through its public API, for each process, whether it has stopped delivering and
   why, sufficiently for an operator to distinguish a deliberate refusal — which recurs identically on restart —
   from a transient failure.
2. Work an implementation performs outside delivering — at startup or between deliveries — SHOULD be bounded in
   time or report progress; no single unavailable dependency SHOULD be able to block startup or delivery
   indefinitely without diagnosis.
3. When an implementation stops, failure to release one resource SHOULD NOT prevent release of the others.
4. An implementation SHOULD support a configurable bound on the causal metadata it will accept on receipt or
   express on send, and on reaching it SHOULD fail closed with its own diagnosis rather than run into a limit of
   the substrate.
5. An implementation SHOULD make the size of each process's causal metadata observable in operation, and SHOULD
   document how that size grows with the shape of the application's topology.
6. A refusal SHOULD name the condition that caused it: an implementation SHOULD NOT report one refusal reason
   where a different recorded reason describes the actual condition.

### Assumptions

1. An implementation MAY assume that a message is a record in a Kafka topic.
2. An implementation MAY assume that a channel is a topic-partition, identified such that a topic deleted and
   recreated under the same name is a different channel.
3. An implementation MAY assume that a message's position on a channel is its Kafka offset.
4. An implementation MAY assume that sending is producing to a topic, and that receiving is fetching from a
   topic-partition.
5. An implementation MAY assume that a process is a Kafka Streams task, whose lifetime spans assignment, revocation and
   restart.
6. An implementation MAY assume that message metadata is carried in Kafka record headers.
7. An implementation MAY assume that a message's key and value are the record's key and value bytes.
8. An implementation MAY assume that a reader is a Kafka consumer and that codecs are Kafka serdes.
9. An implementation MAY assume that Kafka preserves the order of messages within a channel.
10. An implementation MAY assume that retention covers consumer lag, so that a cause is never aged out before its effect
    is delivered.
11. An implementation MAY assume that support for joins is not required.
12. An implementation MAY assume that support for repartitioning channels is not required.
13. An implementation MAY assume that a message carrying causal metadata expresses all of its causes truthfully.
14. An implementation MAY assume that an application's declaration names topics, and stands for every process the host
    induces from it and for the topic-partitions each induced process receives from and sends to.
15. An implementation MAY assume that a channel's earliest retained position is its log-start offset, that the
    substrate reports it and a channel's end position on request, and that querying the substrate for position facts
    is not exchange between processes under Structural 1.
16. An implementation MAY assume that application logic is a pure function of the delivered message and the
    application state passed to it. An effect that bypasses the seam in Structural 3 is invisible to causal order and
    outside every guarantee of this document.
17. An implementation MAY assume that a channel is deleted only once it carries no undelivered obligations: no
    process retains a received-but-undelivered message from it, and no message yet to be received expresses a cause
    on it that its receiver has not already satisfied. Where an implementation detects this assumption breached —
    at minimum, a received-but-undelivered message retained from a channel that no longer exists — Safety 9 states
    its duty. A breach it cannot detect is outside its guarantees, as with Assumption 13.
