# Performance

Parsley adds overhead to your Kafka processing in three distinct places. Understanding where the
cost lives — and how it scales — helps you reason about capacity and tune limits before deploying
to production.

!!! warning "Absolute numbers are infrastructure-specific"
    This page describes **how Parsley's costs scale**, not what they are. The latency figures that
    appear in the tables below are illustrative results from one run on one machine; they are not
    specifications. To get numbers meaningful for your capacity planning, run the benchmark suite
    in your own environment — see [Running the benchmarks](#running-the-benchmarks).

---

## Three latency categories

### 1. Per-record header overhead

Every record Parsley handles requires three operations: deserialise the incoming dependency header,
check whether the frontier satisfies it, and stamp the updated clock onto the outgoing record.

Benchmarking confirms that all three operations scale **linearly with clock width** — the number
of `(topic, partition)` pairs tracked in the causal context. Doubling the clock width roughly
doubles the cost of each operation.

The following results illustrate this scaling in one environment; your numbers will differ:

| Clock width | Deserialise | Dominance check | Serialise |
|---:|---:|---:|---:|
| 1 partition | 27 ns | 2 ns | 16 ns |
| 4 partitions | 131 ns | 18 ns | 55 ns |
| 16 partitions | 406 ns | 60 ns | 211 ns |
| 64 partitions | 1548 ns | 244 ns | 785 ns |

The practical implication is not the specific nanosecond values but the **slope**: cost grows
proportionally with clock width, so keeping the dependency list narrow is the primary lever for
controlling per-record overhead.

!!! tip "Controlling clock width"
    In Streams, Parsley stamps the per-task frontier automatically. The width is bounded by the
    number of source partitions assigned to the task, which is usually small. The wider paths are
    `consumer.frontier()` and `CausalFrontier.toDependencies()`, which carry every partition the
    consumer has ever seen. Prefer `CausalDependencies.fromRecord(trigger)` when you only need to
    express a dependency on a single upstream record — it carries a much narrower clock.

---

### 2. Causal buffering latency

This cost is paid when a causally ready trigger arrives and Parsley releases held records from the
buffer. The drain algorithm has three independent components that each contribute to the latency.

#### Wait-index lookup — logarithmic in buffer depth

Before releasing anything, Parsley queries a RocksDB range index to find which held records were
waiting for the arriving coordinate. Benchmarking confirms this lookup is **O(log n)** in the
total number of buffered records n.

The practical consequence: doubling the buffer adds a roughly fixed increment of latency rather
than doubling it. The drain baseline grows slowly even as the buffer fills — example results from
one run:

| Buffer depth (n) | Drain baseline |
|---:|---:|
| 1 | ~15 µs |
| 8 | ~19 µs |
| 32 | ~25 µs |
| 128 | ~24 µs |
| 512 | ~31 µs |
| 1024 | ~43 µs |

The specific values are not portable, but the shallow slope is the key property: across a
1000× increase in buffer depth, the lookup cost grows by roughly 3×.

#### Per-released record — linear in k

If k held records all depend on the same coordinate, Parsley releases all k of them in one drain
pass. Benchmarking confirms this cost is **O(k)** — linear in the number of records released.
Each released record requires a RocksDB read, a frontier advance, and a forward call.

Example results at n=128 fixed:

| Records released (k) | Drain cost |
|---:|---:|
| 1 | ~24 µs |
| 2 | ~34 µs |
| 4 | ~54 µs |
| 8 | ~92 µs |
| 16 | ~161 µs |
| 32 | ~290 µs |

Note that k=1 already carries a baseline cost from the index lookup; the marginal cost per
additional released record is what scales linearly from there.

#### Cascade depth — linear in r

When a released record itself enables another held record (which enables another, and so on),
Parsley propagates the release through a cascade. Benchmarking confirms this cost is **O(r)**
in the cascade depth r.

Example results at n=128, k=1 fixed:

| Cascade depth (r) | Drain cost |
|---:|---:|
| 1 | ~25 µs |
| 2 | ~35 µs |
| 4 | ~58 µs |
| 8 | ~99 µs |
| 16 | ~178 µs |
| 32 | ~324 µs |

Deep cascades only occur when you have long chains of sequential dependencies — unusual in most
topologies. In a topology where each step depends only on the previous one, r equals the number
of such steps that become simultaneously ready.

#### Putting it together

The full drain cost when a trigger arrives is:

> **O(log n + k + r)**

where n is the total buffer depth, k is the number of records sharing the trigger coordinate,
and r is the cascade depth. For typical workloads with small k and r, the logarithmic index
lookup dominates. Heavy fan-in at a single dependency offset drives up k; strict sequential
pipelines drive up r.

---

### 3. Recovery latency

When Parsley restarts or a Kafka Streams task is reassigned to a new worker, `ParsleyProcessor.init()`
restores state from RocksDB before processing begins.

**Frontier restore** reads a single key and deserialises the frontier. Benchmarking confirms
this is **O(1)** — effectively constant regardless of buffer size:

| Buffer held at crash | Frontier restore |
|---:|---:|
| 1 record | ~461 ns |
| 1024 records | ~439 ns |

**Buffer restore** scans every buffered record to rebuild the wait index. Benchmarking confirms
this is **O(n)** in the number of records held at the time of the crash or reassignment:

| Records held at crash (n) | Buffer restore |
|---:|---:|
| 1 | ~2 µs |
| 8 | ~13 µs |
| 32 | ~58 µs |
| 128 | ~211 µs |
| 512 | ~908 µs |
| 1024 | ~1.9 ms |

The cost grows proportionally with buffer size. In your environment the per-record cost will
differ, but the linear relationship will hold: a buffer twice as large will take roughly twice as
long to restore.

!!! tip "Keep the buffer small for faster restarts"
    A tight `CausalBufferLimit.ofSize(n)` reduces both the number of records held under lag and
    the startup cost if those records were persisted at crash time. The trade-off is more frequent
    policy firings (violations). Size the buffer to the lag you can tolerate, not the maximum
    possible.

---

## Summary

| Category | Complexity | Scales with |
|---|---|---|
| Deserialise header | O(w) | Clock width |
| Dominance check | O(w) | Clock width |
| Serialise header | O(w) | Clock width |
| Buffer drain (index lookup) | O(log n) | Total records in buffer |
| Buffer drain (per-released record) | O(k) | Records sharing the trigger coordinate |
| Cascade propagation | O(r) | Chained release depth |
| Frontier restore on restart | O(1) | — |
| Buffer restore on restart | O(n) | Records held at time of restart |

---

## Running the benchmarks

The benchmark suite lives in `src/test/java/io/parsley/` and can be run with Maven. It uses
`TopologyTestDriver` and does not require a running Kafka cluster by default.

```
mvn -Pbenchmarks test
```

To run a single curve with reduced iterations (smoke test):

```
mvn -Pbenchmarks test \
  "-Dbenchmark.args=-wi 1 -i 3 -f 1 -p clockWidth=4 -p n=8 -p k=1 -p r=1 HeaderEvaluationBenchmark"
```

To export results as JSON for graphing:

```
mvn -Pbenchmarks test \
  "-Dbenchmark.args=-rf json -rff results.json"
```

See [Internals: Benchmark suite](internals/benchmarks.md) for the full parameter reference,
system property overrides, and instructions for running against your own Kafka cluster and storage.
