# Failing closed

Where the guarantee cannot be upheld, a process stops delivering. It stays stopped, and
re-fails on restart, until an operator intervenes — with one recorded exception:
`COVERED_POSITION_FED` is an invariant guard with no known trigger, and a restart resumes
from the committed record without it recurring (the reason's row below, and its exception
message, say so).

A fail-closed event throws `ParsleyFailClosedException` out of the processor, which fails the
task's step. The transaction aborts, and nothing is delivered past the failure.

## Blast radius

The blast radius is the whole process rather than the affected channel alone.

After receiving a message whose metadata cannot be decoded, every subsequent send of that
process risks under-expressing causes. Causes known only from the metadata of received but
undelivered messages must be re-expressed, and an undecodable header conceals exactly those.
A downstream process acting on an under-expressed frontier could then invert causal order.
Stopping the process satisfies the requirement to stop at minimum on the affected channel.

## Triggers

This table says when each reason is raised. What an operator does about it is in
[Runbooks](runbooks.md#runbooks-by-reason), one runbook per reason.

| Reason | Condition |
|---|---|
| `UNDECODABLE_METADATA` | Metadata present and not decodable |
| `POSITIONS_DISCARDED_UNREAD` | Retention discarded positions this process has not read: the consumer's fetch under `auto.offset.reset=none` refused a committed read position below the log-start offset, and the runtime classified the stop. That fetch is the one site — while running, when retention passes a lagging read position; on the first fetch after a start that committed, over an expired offset, the ordering state's covered position plus one — 0 for a partition the state names but never covered, and the substrate's earliest only for a topic the state never named; or after a stop long enough for retention to pass the committed position. There is no engine check and no held-message shape: a held message retention discards is in the ordering changelog and delivers from there once its causes settle. Retention must cover the longest stop and lag, not hold-back time |
| `OUT_OF_ORDER_FEED` | The host fed a channel out of position order within one execution, or fed a channel recorded as no longer existing |
| `COVERED_POSITION_FED` | The host fed a channel at a position this execution's own coverage already records as fed or never arriving, above the session floor — the host's feed and the engine's record contradict each other. An invariant guard with no known trigger: above the session floor, coverage is raised only by this execution's own receipts, which the in-execution order check guards, and by the initialisation's identity report settling a deleted channel to its end, which the dead-channel check guards; a feed inside either is refused first as `OUT_OF_ORDER_FEED`. A restart resumes from the committed record, and the refusal then does not recur |
| `ORDERING_STATE_LOST` | Committed read positions the bootstrap did not write (a previous Kafka Streams execution's stamp, or bare external commits) exist while the ordering-changelog partition behind them holds no records — the topic absent, or its records purged, in whole or for that one partition. If a prior execution ran, the state of its most recent committed step has been lost and resuming would silently under-express every cause delivered before the loss |
| `CHANNEL_REMOVED_WITH_HELD_MESSAGES` | A declaration removed a channel that still holds messages |
| `CHANNEL_IDENTITY_CHANGED` | A topic resolves to an identity other than the one recorded: at start, where a received topic's name now resolves to an identity other than the one the process's state was built against; and at each task initialisation, where the restored state's binding for a name no longer matches, or where the identity check finds a received channel's topic deleted and recreated under its name. A recreation while a task runs is detected at the task's next initialisation — a received topic is assumed not to be deleted and recreated under its name while a process that receives it runs (SPEC Assumption 17) |
| `CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES` | A received topic was deleted while its messages remain held: the identity check at task initialisation finds the topic gone and the task still holds messages from it, whose place in causal order can no longer be preserved. With nothing held, the channel settles to its end instead |
| `TASK_WIDTH_CHANGED` | The task count changed, so ordering state no longer matches its partitioning |
| `UNKNOWN_ORDERING_STATE_FORMAT` | Stored state cannot be trusted: a format version this build cannot read, state present without its version entry (the changelog head has been lost), a corrupt entry — malformed key, wrong-length value, or a held blob whose lengths do not match its bytes — a restored frontier naming the reserved zero topic ID, or held messages restored out of position order |
| `EMISSION_TO_UNDECLARED_CHANNEL` | A handler emitted on a topic outside its process's declared send set. Membership is by topic name: an emission on a declared topic is sent (serialized with the declared channel's serdes) whatever `Channel` instance carried it |
| `STATE_ACCESS_TO_UNDECLARED_STORE` | Application logic read or wrote a store its process never declared, or used a `Store` instance other than the declared one. Every effect target is validated before any write applies, so the refusal leaves no partial step behind — and a read refusal an application catch swallows is latched and rethrown at the delivery frame's next boundary (after the payload's deserializers, after the handler, and after the effects apply), so the step still fails wherever in the frame the read ran |
| `RESERVED_HEADER_USED` | An application header used the reserved prefix |
| `HANDLER_RETURNED_NULL_EFFECTS` | A handler returned `null` instead of `Effects`; return `Effects.none()` for a step that changes nothing |
| `APPLICATION_PAYLOAD_UNDECODABLE` | A payload — a delivered message's, or a stored state value read through the seam — could not be decoded by its declared serde. A state-read failure an application catch swallows is latched and rethrown at the delivery frame's next boundary |
| `APPLICATION_PAYLOAD_UNSERIALIZABLE` | An effect's payload or a state-read key could not be serialized by its declared serde — including a `Channel` or `Store` instance whose types mismatch the declared one. Effects are serialized during planning, before any write applies |
| `SUBSTRATE_MISCONFIGURED` | The substrate is configured in a way the guarantee cannot survive |
| `METADATA_BUDGET_EXCEEDED` | Causal metadata exceeded the configured budget |

An application failure is not a refusal. A handler, serde or codec that throws fails its step
the same way — the transaction aborts, the process stops, and a restart feeds the same
message again — but `status()` reports it with no `refusalReason`, only the failure's
message, since nothing about the guarantee was decided. The remedy is in the application:
catch and record, or fix the code and restart.

Truncation is caught at the fetch. The main consumer runs with `auto.offset.reset=none`, so a
read position below the log start refuses the fetch rather than silently jumping, and the
runtime names the stop `POSITIONS_DISCARDED_UNREAD`. A start with prior state and an expired
committed offset compares nothing against the log start itself: it commits the ordering
state's covered position plus one — where the previous execution would have read next — for
every partition the state covers, 0 for one it names but never covered, and the substrate's
earliest position only for a topic it never named, and the first fetch decides whether
retention still holds it. Nothing else looks. The engine
keeps no check against a log start, and a held message retention discards is not a stop: it
is in the ordering changelog and delivers from there once its causes settle.

## Diagnosis

`Parsley.status()` reports each process. `ProcessStatus.refusalReason()` is present where a
process stopped to preserve the guarantee, and distinguishes that from any other stop. That
includes the two stops the substrate detects that recur identically on restart: the main
consumer meeting a discarded read position under `auto.offset.reset=none` is reported as
`POSITIONS_DISCARDED_UNREAD`, and a held message's persisted form exceeding the changelog's
record limit as `SUBSTRATE_MISCONFIGURED`. A stop a restart resolves carries no reason: a
partition added while the process ran, or a received topic missing at a rebalance — the
runtime classifies it `SOURCE_TOPIC_MISSING`, and the restart then refuses
`CHANNEL_IDENTITY_CHANGED` for a topic recreated under its name, refuses a still-missing one
when it resolves topics, or resumes where a broker's metadata merely lagged.

A held message is not a failure. It means a cause has not arrived, and the diagnosis is which
cause. `ProcessStatus.tasks()` carries that diagnosis for every task the instance runs: per
channel with held messages, how many are held, the position of the head, and each cause the
head is waiting for, named by topic and partition with the position required and the
position the channel has settled to. It also carries the frontier's size in channels and
bytes, against the metadata budget. A task refreshes its entry once per status interval, on
its own thread, and retires it when it closes, so what the status shows is at most one
interval old and never a task this instance no longer runs.

A head with no blockers listed is deliverable and goes on the next drain — the next record
fed, or the next status punctuation. A head whose blocker names a position the channel has
settled below is waiting on that channel's feed. With the required position at or below the
channel's end, the task has not reached the cause yet: lag, or a chain of holds. With it
beyond the end, the cause is not produced yet — inside an open transaction — or the stamp is
stale or forged. A required position no committed record occupies — a marker, an aborted
batch's offset, the log-end offset at stamping time — is an out-of-contract cause, held until
a later record on that channel arrives. A blocker whose settled position is empty names a
channel the task starts at position 0 and has received nothing on yet. Turning a blocker's
shape into an action is [a runbook](runbooks.md#a-message-is-held-and-not-moving).
