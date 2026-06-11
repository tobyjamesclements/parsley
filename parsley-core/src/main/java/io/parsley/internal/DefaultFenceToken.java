package io.parsley.internal;

import io.parsley.FenceToken;
import io.parsley.FenceTokenEncryption;
import io.parsley.VectorClock;
import io.parsley.VectorClockSerialiser;

public record DefaultFenceToken(
        VectorClock vectorClock,
        FenceTokenEncryption encryption,
        VectorClockSerialiser serialiser) implements FenceToken {

    @Override
    public String encode() {
        return encryption.encrypt(serialiser.serialise(vectorClock));
    }
}
