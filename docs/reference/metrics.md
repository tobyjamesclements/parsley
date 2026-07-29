# Metrics

Every stage task registers its metrics in the group `parsley-metrics` on the Kafka Streams
metrics registry, next to the Streams built-ins. They reach JMX under the `kafka.streams`
domain, every configured metrics reporter, and [`CausalStreams.metrics()`](api.md). All of
them carry the tags `thread-id`, `task-id`, and `stage`; the per-topic breakdowns add
`topic`. Sensors are removed when the task closes, so metrics follow task migration.

These metrics exist because holding is invisible to ordinary lag monitoring. The adapter
accepts a record before the gate decides on it, so the stage's committed consumer position
advances past records that are still held. A stalled task can therefore report zero
consumer lag on its own group. Hold depth and hold age are observable only here.

Gauges are sampled by the task's wall-clock punctuator every 500 ms, so a gauge can trail
the event it reflects by up to half a second. Counters are recorded at their event sites.

## Task metrics

These record at the `INFO` metrics recording level, the Kafka default.

| Name | Type | Meaning |
| --- | --- | --- |
| `records-held` | gauge | The number of records currently held across the task's hold queues. |
| `records-held-age-max-ms` | gauge | The wall-clock milliseconds the longest-gating head record has been held, measured from when it became its queue's head. A hold restored at restart ages from the first sample after init. |
| `records-delivered-rate` | rate | The per-second rate of records released through the causal gate. |
| `records-delivered-total` | total | The total number of records released through the causal gate since the task initialised. |
| `replays-skipped-total` | total | The total number of records fed at or below the frontier and discarded. Nonzero is expected only while a rescope-growth refetch replays covered offsets. |
| `truncation-sweeps-skipped-total` | total | The total number of truncation sweeps that failed and skipped their cycle. A climbing value means stamp-side clock entries are not being garbage-collected. |
| `stamp-offset-entries` | gauge | The number of offset entries in the stamp-side clock. |
| `stamp-sequence-entries` | gauge | The number of sequence entries in the stamp-side clock, claims not yet resolved to offsets. |

## Per-topic metrics

These record at the `DEBUG` metrics recording level. Enable them with
`metrics.recording.level=DEBUG` in the application properties; at the default level the
sensors exist but do not record.

| Name | Type | Meaning |
| --- | --- | --- |
| `records-held` | gauge | The number of records currently held on this source topic's channel. |
| `records-held-age-max-ms` | gauge | The wall-clock milliseconds this channel's head record has been held. |

## Reading the stall pair

`records-held` alone distinguishes nothing: any skew between cause and effect topics holds
records transiently, and the gauge returns to zero as causes arrive. The stall signal is
`records-held-age-max-ms` growing without bound, which means a head record is waiting on a
cause that is not arriving. The per-topic breakdown names the convoyed channel, and the
head's dependency clock is in the `DEBUG` protocol log. The cure is fixing the lagging
cause, never skipping the head ([the contract](../guide/expectations.md#operating-it));
retention on the cause topic bounds how long the wait can succeed.

The width gauges guard the other slow failure. Stamp-side clock entries are freed by the
log-start truncation sweep ([truncation](../design/architecture.md#truncation-log-start-stability)),
so `stamp-offset-entries` growing against a steady workload, or
`truncation-sweeps-skipped-total` climbing, means clock and header overhead is accumulating.
