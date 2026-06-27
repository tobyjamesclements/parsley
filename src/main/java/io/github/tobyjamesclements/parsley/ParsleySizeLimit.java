package io.github.tobyjamesclements.parsley;

/**
 * Evicts the buffer when its size reaches {@code messages}.
 */
record ParsleySizeLimit(int messages) implements CausalBoundedBufferLimit {
    ParsleySizeLimit {
        if (messages <= 0) {
            throw new IllegalArgumentException("messages must be positive, was " + messages);
        }
    }
}
