# Benchmark suite

A JMH performance-characterisation suite covering the three areas of Parsley's algorithmic
complexity. The goal is to produce **complexity curves**, not absolute latency figures. Complexity
curves are reproducible across environments; absolute numbers are infrastructure-specific and must
be generated in the target environment to be meaningful.

See [Performance](../performance.md) for an explanation of what each curve measures and what the
results imply for capacity planning.

---

## What each benchmark class measures

### `HeaderEvaluationBenchmark` — per-record overhead, O(w)

Three operations measured independently across clock width `w` (number of tracked partitions):

| Method | Operation | Expected complexity |
|---|---|---|
| `deserialize` | Parse causal dependencies from incoming record header | O(w) |
| `dominanceCheck` | Compare parsed deps against local frontier for admissibility | O(w) |
| `serialize` | Write current frontier to outgoing record header | O(w) |

Produces three curves on a shared x-axis showing which operation dominates the per-record overhead
and how each scales with topology width.

### `BufferReleaseBenchmark` — causal buffering latency, O(log n + k + r)

Three sub-benchmarks each isolating one dimension of the drain algorithm. Each method varies one
parameter while holding the others fixed:

| Method | Param varied | Fixed | Expected complexity |
|---|---|---|---|
| `bufferSize` | `n` (total records in buffer) | k=1, r=1 | O(log n) |
| `positionalOccupancy` | `k` (records sharing the trigger coordinate) | n=128, r=1 | O(k) |
| `cascadeDepth` | `r` (cascade hops) | n=128, k=1 | O(r) |

All three methods use changelog-backed RocksDB stores via `TopologyTestDriver`.

JMH runs all combinations of the three `@Param` dimensions. To isolate a single curve, restrict
parameters at run time — see [Isolating a single curve](#isolating-a-single-curve).

### `StateRestorationBenchmark` — recovery latency

Two operations measured across buffer size `n`, reproducing the full cost of
`ParsleyProcessor.init()` when non-empty state is found in RocksDB after a restart or task
reassignment:

| Method | Operation | Expected complexity |
|---|---|---|
| `frontierRestore` | Single RocksDB point read + `ParsleyClock.fromBytes()` | O(1) |
| `bufferRestore` | `RocksBufferStore` constructor scan + `ParsleyEngine` candidate-index rebuild | O(n) |

---

## System properties

| Property | Default | Description |
|---|---|---|
| `parsley.bench.bootstrap.servers` | TestContainers-managed broker | Kafka bootstrap address |
| `parsley.bench.state.store.dir` | System temp dir | Root for RocksDB state directories |
| `parsley.bench.replication.factor` | `1` | Topic replication factor (future use) |
| `parsley.bench.partition.count` | `1` | Topic partition count (future use) |

---

## Running the benchmarks

### Default (TestContainers, no configuration needed)

```
mvn -Pbenchmarks test
```

TestContainers starts a Kafka broker automatically. Docker must be available.

### Against your own cluster

```
mvn -Pbenchmarks test \
  -Dparsley.bench.bootstrap.servers=my-broker:9092 \
  -Dparsley.bench.state.store.dir=/mnt/nvme/parsley-bench
```

No recompile or code change required.

### Isolating a single curve

Pass JMH flags via `-Dbenchmark.args`. The benchmark regexp is a positional argument (not a
flag):

```
# n-curve only (bufferSize benchmark, k=1, r=1)
mvn -Pbenchmarks test \
  "-Dbenchmark.args=BufferReleaseBenchmark.bufferSize -p k=1 -p r=1"
```

Alternatively, modify `BenchmarkRunner.java`'s `OptionsBuilder` directly:

```java
.include("BufferReleaseBenchmark.bufferSize")
.param("k", "1")
.param("r", "1")
```

### Quick smoke run

```
mvn -Pbenchmarks test \
  "-Dbenchmark.args=-wi 1 -i 1 -f 1 -p clockWidth=4 -p n=8 -p k=1 -p r=1 HeaderEvaluationBenchmark"
```

### Exporting results for graphing

Pass `-rf json -rff results.json` via `benchmark.args`:

```
mvn -Pbenchmarks test \
  "-Dbenchmark.args=-rf json -rff results.json"
```

The JSON output can be visualised with [JMH Visualizer](https://jmh.morethan.io/) or processed
with any JSON-capable tool (Python/pandas, R, jq, etc.).

---

## Interpretation

**Complexity curves are the primary output.** Each benchmark is designed to isolate one algorithmic
dimension while holding others constant. The slope of the resulting curve against the varying
parameter confirms (or refutes) the theoretical complexity claim.

**Absolute latency figures are infrastructure-specific.** The numbers produced by this suite
reflect your hardware, storage class, and JVM GC configuration; they are not directly comparable
to numbers generated on different infrastructure. To produce figures meaningful for a
capacity-planning decision, run this suite in your own environment against your own Kafka cluster
and storage.
