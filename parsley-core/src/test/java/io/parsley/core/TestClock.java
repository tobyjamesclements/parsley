package io.parsley.core;

import io.parsley.VectorClock;
import io.parsley.VectorClocks;

import java.util.Map;

record TestClock(Map<String, Long> positions) implements VectorClock<TestClock> {

    static TestClock empty() {
        return new TestClock(Map.of());
    }

    static TestClock at(String partition, long position) {
        return new TestClock(Map.of(partition, position));
    }

    TestClock advance(String partition, long position) {
        return new TestClock(VectorClocks.advance(positions, partition, position));
    }

    @Override
    public boolean satisfiedBy(TestClock frontier) {
        return VectorClocks.satisfied(positions, frontier.positions());
    }

    @Override
    public TestClock merge(TestClock other) {
        return new TestClock(VectorClocks.merge(positions, other.positions()));
    }
}
