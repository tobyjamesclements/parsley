package io.parsley.crypto.jdk.aes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class JdkFenceTokenEncryptionTest {

    @Test
    void roundTripRestoresOriginalBytes() {
        JdkFenceTokenEncryption enc = new JdkFenceTokenEncryption();
        byte[] original = "hello, parsley!".getBytes();
        assertArrayEquals(original, enc.decrypt(enc.encrypt(original)));
    }

    @Test
    void differentIvEachEncryption() {
        JdkFenceTokenEncryption enc = new JdkFenceTokenEncryption();
        byte[] data = "same data".getBytes();
        assertNotEquals(enc.encrypt(data), enc.encrypt(data));
    }

    @Test
    void wrongKeyThrowsOnDecrypt() {
        JdkFenceTokenEncryption enc1 = new JdkFenceTokenEncryption();
        JdkFenceTokenEncryption enc2 = new JdkFenceTokenEncryption();
        String ciphertext = enc1.encrypt("secret".getBytes());
        assertThrows(IllegalStateException.class, () -> enc2.decrypt(ciphertext));
    }

    @Test
    void outputIsBase64UrlWithoutPadding() {
        JdkFenceTokenEncryption enc = new JdkFenceTokenEncryption();
        String token = enc.encrypt("data".getBytes());
        assertFalse(token.contains("+"), "must not contain +");
        assertFalse(token.contains("/"), "must not contain /");
        assertFalse(token.contains("="), "must not contain padding");
    }

    @Test
    void emptyBytesRoundTrip() {
        JdkFenceTokenEncryption enc = new JdkFenceTokenEncryption();
        assertArrayEquals(new byte[0], enc.decrypt(enc.encrypt(new byte[0])));
    }

    @Test
    void decryptEmptyStringThrows() {
        JdkFenceTokenEncryption enc = new JdkFenceTokenEncryption();
        // empty Base64 decodes to 0 bytes; System.arraycopy then throws copying 12 IV bytes
        assertThrows(Exception.class, () -> enc.decrypt(""));
    }

    @Test
    void decryptInvalidBase64Throws() {
        JdkFenceTokenEncryption enc = new JdkFenceTokenEncryption();
        assertThrows(Exception.class, () -> enc.decrypt("not!!valid!!base64"));
    }

    @Test
    void providerReturnsSameSingletonInstance() {
        assertSame(JdkFenceTokenEncryption.provider(), JdkFenceTokenEncryption.provider());
    }

    @Test
    void decryptWithTamperedIvThrows() {
        JdkFenceTokenEncryption enc = new JdkFenceTokenEncryption();
        byte[] decoded = Base64.getUrlDecoder().decode(enc.encrypt("data".getBytes()));
        Arrays.fill(decoded, 0, 12, (byte) 0);
        String tampered = Base64.getUrlEncoder().withoutPadding().encodeToString(decoded);
        assertThrows(IllegalStateException.class, () -> enc.decrypt(tampered));
    }
}
