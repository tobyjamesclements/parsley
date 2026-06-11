package io.parsley;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PartitionTest {

    @Test
    void nullTopicThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new Partition(null, 0));
    }

    @Test
    void negativePartitionThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new Partition("topic", -1));
    }

    @Test
    void zeroPartitionIsValid() {
        Partition p = new Partition("topic", 0);
        assertEquals("topic", p.topic());
        assertEquals(0, p.partition());
    }

    @Test
    void equalPartitionsAreEqual() {
        assertEquals(new Partition("orders", 3), new Partition("orders", 3));
    }

    @Test
    void equalPartitionsHaveSameHashCode() {
        assertEquals(new Partition("orders", 3).hashCode(), new Partition("orders", 3).hashCode());
    }

    @Test
    void differentTopicNotEqual() {
        assertNotEquals(new Partition("a", 0), new Partition("b", 0));
    }

    @Test
    void differentPartitionNumberNotEqual() {
        assertNotEquals(new Partition("topic", 0), new Partition("topic", 1));
    }
}
