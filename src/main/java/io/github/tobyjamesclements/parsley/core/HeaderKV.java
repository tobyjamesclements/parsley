package io.github.tobyjamesclements.parsley.core;

/** A record header as received or to be sent: application metadata travelling with the message, never its key or value. */
public record HeaderKV(String key, byte[] value) {
    public HeaderKV {
        if (key == null) {
            throw new NullPointerException("header key");
        }
    }
}
