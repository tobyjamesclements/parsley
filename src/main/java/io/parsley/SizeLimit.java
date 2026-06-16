package io.parsley;

/**
 * Evicts the buffer when its size reaches {@code messages}.
 */
record SizeLimit(int messages) implements CausalBufferLimit {
    SizeLimit {
        if (messages <= 0) {
            throw new IllegalArgumentException("messages must be positive, was " + messages);
        }
    }
}
