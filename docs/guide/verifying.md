# Verifying your application

[The contract](expectations.md) states what Parsley expects of an application; this page is
how the mechanically checkable clauses run in your test suite instead of being remembered.
The docs describe, the probes verify. Nothing here replaces the runtime's fail-closed checks
— probes move discovery from deploy time to test time.

## Test-time probes

`ContractProbes.probe` drives your application's own `testTopology()` under
`TopologyTestDriver` — broker-less, so it runs in plain CI — with sample records you supply,
at least one per source topic. `kafka-streams-test-utils` must be on the test classpath (it
is an optional dependency of Parsley, so your test scope declares it, production classpaths
never see it).

```java
@Test
void applicationMeetsTheContract(@TempDir Path stateDir) {
    ContractProbes.probe(
            Parsley.named("settlements-app", settlement, balances),
            ContractProbes.Samples.of()
                    .on(orders, "o-1", sampleOrder())
                    .on(payments, "p-1", samplePayment()),
            stateDir
    ).assertOk();
}
```

`assertOk()` throws with every failure at once, each finding naming the probe, what it
observed, and the contract clause it checks:

| Probe | Checks | Clause |
|---|---|---|
| `key-codec` | Key codecs are deterministic and canonical on the samples: encode twice agrees, decode-then-encode returns the same bytes. The encoded bytes choose the partition and key the fold state. | [Key codecs are identity](codecs.md#key-codecs-are-identity) |
| `stamps` | Every record observed on a sink carries parseable `vc`, `vc-sender`, `vc-seq` headers. | [Stamps](expectations.md#what-parsley-expects-of-you) |
| `sink-decode` | Every observed output decodes with its sink topic's own codecs — the bytes the next hop's consumer will decode. | [Codecs](codecs.md) |
| `declared-sinks` | No handler path emits to an undeclared sink. | [Logic](expectations.md#what-parsley-expects-of-you) |
| `logic` | Handlers and folds are total on their samples — a throwing handler is a finding, not a stack trace to reverse-engineer. | [Error handling](error-handling.md) |
| `samples` | Every sample names a topic some stage consumes — a misconfigured probe run fails, it does not silently pass. | [Topics](expectations.md#what-parsley-expects-of-you) |

Findings that are gaps rather than violations arrive as **notes**: a source topic with no
sample (its handler path went unexercised), a declared sink no sample reached, the tick
topics the deployment must create, and the store names each stage pins — the reminder that
stage names and the application id stay stable across deployments. A report with notes and
no failures passed, with stated blind spots.

Ticking stages tick under the probe: the driver's wall clock advances past the widest
declared interval, and the emitted ticks pass through the same stamp checks as every other
record.

## Cluster probes

The clauses about the cluster itself — topics exist before start, a stage's sources agree on
partition count (the mechanical half of [co-partitioning](expectations.md#what-parsley-expects-of-you);
the key-bytes half is yours), tick topics carry exactly the partition count of the stage's
widest source — cannot be seen by a test driver. `ContractProbes.probeCluster` asks a live
cluster, read-only, without starting the application:

```java
try (Admin admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
    ContractProbes.probeCluster(
            Parsley.named("settlements-app", settlement, balances), admin).assertOk();
}
```

Run it from a deploy pipeline against the target environment; what it checks is exactly what
`Parsley.streams` will fail closed on at init, minus the surprise.

## What probes cannot check

- **Co-partitioning's key half.** That related keys land on the same partition number of
  every source is a property of your producers' keys and encodings; the probe checks the
  partition-count half only.
- **Plain producers outside the topology.** A producer that should stamp with a
  [`CausalClock`](clients.md) but does not is invisible here — the probe sees only the
  topology's own outputs. An unstamped record claims nothing; safe for itself, invisible to
  downstream ordering.
- **Purity and closed effects.** A handler that writes to an external system looks pure to
  every black-box check; the [Logic clause](expectations.md#what-parsley-expects-of-you)
  stays yours to uphold.
- **The causal guarantee itself.** That is Parsley's obligation, discharged by
  [its own verification](../design/verification.md), not something each application re-proves.
