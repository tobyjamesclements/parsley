# Failing closed

Where the guarantee cannot be upheld, a process stops delivering. It stays stopped, and
re-fails on restart, until an operator intervenes — with one recorded exception:
`COVERED_POSITION_FED` raised because this execution was superseded does not recur, and a
restart recovers it (the reason's row below, and its exception message, say so).

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

| Reason | Condition |
|---|---|
| `UNDECODABLE_METADATA` | Metadata present and not decodable |
| `POSITIONS_DISCARDED_UNREAD` | Retention discarded positions this process has not delivered: a read position below the log-start offset, whether detected by the engine's log-start check mid-run or at start when a lost read position would be re-established beyond the ordering state's covered position; or a held message — received, not yet delivered — whose position the log start has crossed, since its senders may since have pruned it from the causes they express and its place in causal order can no longer be preserved (the retention counterpart of `CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES`). Retention must cover hold-back time |
| `OUT_OF_ORDER_FEED` | The host fed a channel out of position order within one execution, or fed a channel recorded as no longer existing |
| `COVERED_POSITION_FED` | A message arrived at a position a read-position report had already covered as fed-or-never-arriving. Either the report was false, or this execution was superseded and its seed round observed its successor's committed progress — a superseded execution's step cannot commit, a restart recovers, and the refusal then does not recur |
| `ORDERING_STATE_LOST` | Committed read positions the bootstrap did not write (a previous Kafka Streams execution's stamp, or bare external commits) exist while the ordering-changelog partition behind them holds no records — the topic absent, or its records purged, in whole or for that one partition. If a prior execution ran, the state of its most recent committed step has been lost and resuming would silently under-express every cause delivered before the loss |
| `CHANNEL_REMOVED_WITH_HELD_MESSAGES` | A declaration removed a channel that still holds messages |
| `CHANNEL_IDENTITY_CHANGED` | A topic resolves to an identity other than the one recorded |
| `CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES` | A received topic was deleted while its messages remain held |
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

Truncation is caught three times. The main consumer runs with `auto.offset.reset=none`, so an
out-of-range position kills the task rather than silently jumping; the engine independently
fails when a log-start fact exceeds the channel's settled position plus one — `fedUpTo + 1`
while nothing is held, the head of the hold-back buffer while something is, so a discarded
held message is caught as well as a discarded unread one; and a start that would re-establish
an expired read position beyond the ordering state's covered position refuses before committing
it — the one shape the other two cannot see, because the re-established position is in range
and its own report would advance the coverage the engine's check compares against.

## Diagnosis

`Parsley.status()` reports each process. `ProcessStatus.refusalReason()` is present where a
process stopped to preserve the guarantee, and distinguishes that from any other stop. That
includes the two stops the substrate detects that recur identically on restart: the main
consumer meeting a discarded read position under `auto.offset.reset=none` is reported as
`POSITIONS_DISCARDED_UNREAD`, and a held message's persisted form exceeding the changelog's
record limit as `SUBSTRATE_MISCONFIGURED`. A stop a restart resolves — a partition added
while the process ran — carries no reason.

A held message is not a failure. It means a cause has not arrived, and the diagnosis is which
cause. `ProcessStatus.tasks()` carries that diagnosis for every task the instance runs: per
channel with held messages, how many are held, the position of the head, and each cause the
head is waiting for, named by topic and partition with the position required and the
position the channel has settled to. It also carries the frontier's size in channels and
bytes, against the metadata budget, and how long ago broker position facts were last
applied. A task refreshes its entry once per facts interval, on its own thread, and retires
it when it closes, so what the status shows is at most one interval old and never a task
this instance no longer runs.

A head with no blockers listed is deliverable and goes on the next drain. A head whose
blocker names a position the channel has settled below is waiting on that channel's feed to
reach the record the cause names; a blocker whose settled position is empty names a channel
this task has heard nothing of yet.
