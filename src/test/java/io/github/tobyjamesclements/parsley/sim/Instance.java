package io.github.tobyjamesclements.parsley.sim;

import java.util.List;
import java.util.Set;

import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.HeaderKV;

/**
 * One execution of a simulated process, with the state it keeps across restarts.
 */
public final class Instance {
    final ChannelId channel;
    final long position;
    final String uid;
    final byte[] key;
    final byte[] value;
    final List<HeaderKV> headers;
    final Causes meta;
    final Set<Instance> trueCauses;
    /**
     * The message timestamp the substrate hands the engine. Deliberately not the position:
     * an engine that read a timestamp where it should read a position — in the store's held
     * encoding, in a restore, or in the decision, which Structural 7 forbids from consulting
     * timestamps at all — would pass every run in which the two coincide.
     */
    final long timestamp;

    Instance(ChannelId channel, long position, String uid, byte[] key, byte[] value,
             List<HeaderKV> headers, Causes meta, Set<Instance> trueCauses) {
        this.channel = channel;
        this.position = position;
        this.timestamp = 1_000_000_000L + (uid.hashCode() & 0xFFFF) * 1_000L + (position * 7L) % 1_000L;
        this.uid = uid;
        this.key = key;
        this.value = value;
        this.headers = List.copyOf(headers);
        this.meta = meta;
        this.trueCauses = Set.copyOf(trueCauses);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Instance other && position == other.position && channel.equals(other.channel);
    }

    @Override
    public int hashCode() {
        return channel.hashCode() * 31 + Long.hashCode(position);
    }

    @Override
    public String toString() {
        return uid + "(" + channel + "@" + position + ")";
    }
}
