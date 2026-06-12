package io.parsley.core.internal;

import io.parsley.FenceToken;
import io.parsley.FenceTokenEncryption;
import io.parsley.VectorClock;
import io.parsley.VectorClockSerialiser;

public record DefaultFenceToken<T extends VectorClock<T>>(
        T vectorClock,
        FenceTokenEncryption encryption,
        VectorClockSerialiser<T> serialiser) implements FenceToken<T> {

    @Override
    public String encode() {
        return encryption.encrypt(serialiser.serialise(vectorClock));
    }
}
