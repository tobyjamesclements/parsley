# Failing closed

Where the guarantee cannot be upheld, a process stops delivering. It stays stopped, and
re-fails on restart, until an operator intervenes.

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
| `POSITIONS_DISCARDED_UNREAD` | A read position at or below the log-start offset, so discarded positions cannot be assumed empty |
| `OUT_OF_ORDER_FEED` | The host fed a channel out of position order |
| `CHANNEL_REMOVED_WITH_HELD_MESSAGES` | A declaration removed a channel that still holds messages |
| `CHANNEL_IDENTITY_CHANGED` | A topic resolves to an identity other than the one recorded |
| `CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES` | A received topic was deleted while its messages remain held |
| `TASK_WIDTH_CHANGED` | The task count changed, so ordering state no longer matches its partitioning |
| `UNKNOWN_ORDERING_STATE_FORMAT` | Stored state cannot be trusted: a format version this build cannot read, state present without its version entry (the changelog head has been lost), a corrupt entry — malformed key, wrong-length value, or a held blob whose lengths do not match its bytes — or held messages restored out of position order |
| `EMISSION_TO_UNDECLARED_CHANNEL` | A handler emitted on a topic outside its process's declared send set. Membership is by topic name: an emission on a declared topic is sent (serialized with the declared channel's serdes) whatever `Channel` instance carried it |
| `STATE_ACCESS_TO_UNDECLARED_STORE` | Application logic read or wrote a store its process never declared, or used a `Store` instance other than the declared one. Every effect target is validated before any write applies, so the refusal leaves no partial step behind — and a read refusal an application catch swallows is latched and rethrown when the handler returns, so the step still fails |
| `RESERVED_HEADER_USED` | An application header used the reserved prefix |
| `APPLICATION_PAYLOAD_UNDECODABLE` | A payload could not be decoded by its declared serde |
| `APPLICATION_PAYLOAD_UNSERIALIZABLE` | An effect's payload could not be serialized by its declared serde — including a `Channel` or `Store` instance whose types mismatch the declared one. Effects are serialized during planning, before any write applies |
| `SUBSTRATE_MISCONFIGURED` | The substrate is configured in a way the guarantee cannot survive |
| `METADATA_BUDGET_EXCEEDED` | Causal metadata exceeded the configured budget |

Truncation is caught twice. The main consumer runs with `auto.offset.reset=none`, so an
out-of-range position kills the task rather than silently jumping, and the engine
independently fails when a log-start fact exceeds `fedUpTo + 1`.

## Diagnosis

`Parsley.status()` reports each process. `ProcessStatus.refusalReason()` is present where a
process stopped to preserve the guarantee, and distinguishes that from any other stop.

A held message is not a failure. It means a cause has not arrived, and the diagnosis is which
cause: `Deliverability.Held` names every outstanding blocker, with the position required and
the position reached.
