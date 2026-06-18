package io.parsley;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Entry point for the Parsley JMH benchmark suite.
 *
 * <p>Run via Maven:
 * <pre>
 *   mvn -Pbenchmarks test
 * </pre>
 *
 * <p>See the Benchmark suite page in the Parsley documentation for full usage, system-property
 * overrides, and result-export instructions.
 */
public class BenchmarkRunner {

    public static void main(String[] args) throws Exception {
        // Parse any extra JMH flags supplied via -Dbenchmark.args="..."
        String argsStr = System.getProperty("benchmark.args", "").strip();
        String[] extraArgs = argsStr.isEmpty() ? new String[0] : argsStr.split("\\s+");
        CommandLineOptions cmdOpts;
        try {
            cmdOpts = new CommandLineOptions(extraArgs);
        } catch (CommandLineOptionException e) {
            System.err.println("Invalid benchmark.args: " + e.getMessage());
            System.exit(1);
            return;
        }

        ChainedOptionsBuilder builder = new OptionsBuilder()
                .parent(cmdOpts)
                // Propagate infra properties so each JMH-forked JVM has them too.
                // benchmark.args is NOT propagated — it is already applied in the driver JVM above.
                // bootstrap.servers defaults to "dummy:1234" so forked JVMs don't start TestContainers;
                // the benchmarks use TopologyTestDriver and do not need a real broker.
                .jvmArgsAppend(
                        "--enable-native-access=ALL-UNNAMED",
                        "-Dparsley.bench.bootstrap.servers="
                                + System.getProperty("parsley.bench.bootstrap.servers", "dummy:1234"),
                        "-Dparsley.bench.state.store.dir="
                                + System.getProperty("parsley.bench.state.store.dir", ""));

        // Add default includes only when the user did not supply -include via benchmark.args.
        // If the builder adds includes they would override the parent's (cmdOpts) include list,
        // so we only add them when cmdOpts provides none.
        if (cmdOpts.getIncludes().isEmpty()) {
            builder.include(HeaderEvaluationBenchmark.class.getSimpleName())
                   .include(BufferReleaseBenchmark.class.getSimpleName())
                   .include(StateRestorationBenchmark.class.getSimpleName());
        }

        Options opts = builder.build();
        new Runner(opts).run();
    }
}
