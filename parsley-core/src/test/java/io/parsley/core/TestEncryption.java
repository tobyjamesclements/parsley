package io.parsley.core;

import io.parsley.FenceTokenEncryption;

import java.util.Base64;

/** Base64 stand-in for real encryption; registered via META-INF/services. */
public final class TestEncryption implements FenceTokenEncryption {

    @Override
    public String encrypt(byte[] data) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(data);
    }

    @Override
    public byte[] decrypt(String encoded) {
        return Base64.getUrlDecoder().decode(encoded);
    }
}
