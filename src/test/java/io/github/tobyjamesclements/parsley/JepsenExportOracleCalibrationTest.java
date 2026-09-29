package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import io.github.tobyjamesclements.parsley.EngineTestFactory.SabotageMode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Calibrates the export-based checker against the simulator: it must pass every honest
 * run and catch every sabotage mode from the export alone, which is all a cluster run
 * yields. The same exports, written by {@link JepsenSimulatorExport#writeCalibrationSet},
 * calibrate the Clojure checker in {@code parsley-jepsen}.
 */
class JepsenExportOracleCalibrationTest {
    static final int HONEST_SEEDS = 60;
    static final int SWEEP_SEEDS = 80;

    /** Every honest seed replays clean: the export carries no false positive. */
    @Test
    void honestRunsReplayClean() {
        List<String> failures = new ArrayList<>();
        for (long seed = 1; seed <= HONEST_SEEDS; seed++) {
            Map.Entry<JepsenExport, Scenario.Result> run = JepsenSimulatorExport.run(seed, SabotageMode.NONE,
                    SimProcess.HostFault.NONE);
            assertTrue(run.getValue().clean(), () -> "the simulator itself must be clean on seed " + run.getKey().seed);
            JepsenExportOracle.Verdict verdict = JepsenExportOracle.check(run.getKey());
            if (!verdict.clean()) {
                failures.add("seed " + seed + ":\n    " + String.join("\n    ", verdict.violations()));
            }
        }
        assertEquals(List.of(), failures, "honest runs must replay without a violation");
    }

    /** The honest dead-holds scenario refuses, and the refusal is justified by the kill. */
    @Test
    void honestDeadHoldsScenarioReplaysClean() {
        JepsenExportOracle.Verdict verdict = JepsenExportOracle.check(
                JepsenSimulatorExport.deadHoldsScenario(SabotageMode.NONE));
        assertEquals(List.of(), verdict.violations());
    }

    /** The constructed inversion under DELIVER_PAST_DEAD_HOLDS is caught from the export. */
    @Test
    void deliveringPastDeadHoldsIsCaughtFromTheExport() {
        JepsenExportOracle.Verdict verdict = JepsenExportOracle.check(
                JepsenSimulatorExport.deadHoldsScenario(SabotageMode.DELIVER_PAST_DEAD_HOLDS));
        assertTrue(verdict.violations().stream().anyMatch(v -> v.startsWith("Safety 1")),
                () -> "expected a Safety 1 violation, got: " + verdict.violations());
    }

    /**
     * Every sabotage mode and the host fault are caught from the export on a margin of the
     * seeds the simulator's own oracle catches them on. The margin is recorded here so a
     * change to the export or the replay that blinds the checker to a mode fails loudly.
     */
    @Test
    void everySabotageModeIsCaughtFromTheExport() {
        Map<SabotageMode, Integer> floors = new EnumMap<>(SabotageMode.class);
        for (SabotageMode mode : SabotageMode.values()) {
            if (mode != SabotageMode.NONE && mode != SabotageMode.DELIVER_PAST_DEAD_HOLDS) {
                floors.put(mode, 1);
            }
        }
        List<String> report = new ArrayList<>();
        List<String> misses = new ArrayList<>();
        floors.forEach((mode, floor) -> {
            int simulatorCaught = 0;
            int exportCaught = 0;
            for (long seed = 1; seed <= SWEEP_SEEDS; seed++) {
                Map.Entry<JepsenExport, Scenario.Result> run = JepsenSimulatorExport.run(seed, mode,
                        SimProcess.HostFault.NONE);
                if (run.getValue().clean()) {
                    continue;
                }
                simulatorCaught++;
                if (!JepsenExportOracle.check(run.getKey()).clean()) {
                    exportCaught++;
                }
            }
            report.add(mode + ": simulator " + simulatorCaught + ", export " + exportCaught + " of " + SWEEP_SEEDS);
            if (exportCaught < floor) {
                misses.add(mode + " caught from the export on " + exportCaught + " seeds (floor " + floor + ")");
            }
        });
        int hostSimulator = 0;
        int hostExport = 0;
        for (long seed = 1; seed <= SWEEP_SEEDS; seed++) {
            Map.Entry<JepsenExport, Scenario.Result> run = JepsenSimulatorExport.run(seed, SabotageMode.NONE,
                    SimProcess.HostFault.RESET_PAST_LOG_START);
            if (run.getValue().clean()) {
                continue;
            }
            hostSimulator++;
            if (!JepsenExportOracle.check(run.getKey()).clean()) {
                hostExport++;
            }
        }
        report.add("RESET_PAST_LOG_START: simulator " + hostSimulator + ", export " + hostExport + " of " + SWEEP_SEEDS);
        if (hostExport < 1) {
            misses.add("RESET_PAST_LOG_START caught from the export on " + hostExport + " seeds");
        }
        System.out.println("export calibration:\n  " + String.join("\n  ", report));
        assertEquals(List.of(), misses, () -> "calibration:\n  " + String.join("\n  ", report));
    }

    /** The export survives its EDN round trip with the same verdict. */
    @Test
    void exportRoundTripsThroughEdn() {
        Map.Entry<JepsenExport, Scenario.Result> run = JepsenSimulatorExport.run(3, SabotageMode.IGNORE_CAUSES,
                SimProcess.HostFault.NONE);
        JepsenExport reread = JepsenExport.read(run.getKey().write());
        assertEquals(run.getKey().write(), reread.write(), "the EDN must round-trip byte for byte");
        assertEquals(JepsenExportOracle.check(run.getKey()).violations(), JepsenExportOracle.check(reread).violations());
    }

    /** The calibration set writes, with honest files expected clean and every mode present. */
    @Test
    void calibrationSetWrites(@TempDir Path dir) throws Exception {
        List<JepsenSimulatorExport.Calibration> set = JepsenSimulatorExport.writeCalibrationSet(dir, 40);
        assertTrue(set.stream().anyMatch(JepsenSimulatorExport.Calibration::expectClean));
        for (SabotageMode mode : SabotageMode.values()) {
            if (mode == SabotageMode.NONE) {
                continue;
            }
            assertTrue(set.stream().anyMatch(c -> c.mode().equals(mode.name())),
                    () -> "the calibration set must carry a run for " + mode);
        }
        assertFalse(set.stream().anyMatch(c -> c.expectClean() && !JepsenExportOracle.check(
                readBack(dir, c)).clean()), "every file expected clean must replay clean from disk");
    }

    private static JepsenExport readBack(Path dir, JepsenSimulatorExport.Calibration calibration) {
        try {
            return JepsenExport.readFrom(dir.resolve(calibration.file()));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
