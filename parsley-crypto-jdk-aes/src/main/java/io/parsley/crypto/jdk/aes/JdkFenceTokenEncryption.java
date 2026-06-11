package io.parsley.crypto.jdk.aes;

import io.parsley.FenceTokenEncryption;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * AES-256-GCM implementation of {@link FenceTokenEncryption} backed by the standard JDK
 * {@code javax.crypto} API.
 *
 * <p>This is the default implementation shipped with Parsley and is registered as a
 * {@link java.util.ServiceLoader} provider via
 * {@code META-INF/services/io.parsley.FenceTokenEncryption}.
 *
 * <h2>Key lifecycle</h2>
 * <p>The no-arg constructor generates a fresh, cryptographically random 256-bit AES key
 * at instantiation time. The {@link java.util.ServiceLoader}-managed singleton (returned by
 * {@link #provider()}) therefore uses an <em>ephemeral, per-JVM key</em>: tokens encoded with
 * {@link io.parsley.FenceToken#of} are only decodable within the same JVM process.
 *
 * <p>For scenarios where tokens must survive process restarts or cross JVM boundaries,
 * inject a persistent key via {@link #JdkFenceTokenEncryption(SecretKey)}.
 *
 * <h2>Wire format</h2>
 * <p>{@link #encrypt} returns a URL-safe Base64 string encoding:
 * <pre>[12-byte IV][AES-GCM ciphertext + 128-bit auth tag]</pre>
 * A fresh {@link SecureRandom} IV is generated per encryption to ensure ciphertext
 * uniqueness.
 */
public final class JdkFenceTokenEncryption implements FenceTokenEncryption {

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int IV_LENGTH_BYTES = 12;
    private static final int TAG_LENGTH_BITS = 128;

    // Singleton for ServiceLoader — single-JVM ephemeral key shared across of()/decode() calls
    private static final JdkFenceTokenEncryption SINGLETON = new JdkFenceTokenEncryption();

    /**
     * Returns the shared singleton instance used by {@link java.util.ServiceLoader}.
     *
     * <p>This instance uses an ephemeral key generated at JVM startup. All
     * {@link io.parsley.FenceToken#of} and {@link io.parsley.FenceToken#decode} calls within
     * the same JVM share this key.
     *
     * @return the singleton {@code JdkFenceTokenEncryption}
     */
    public static FenceTokenEncryption provider() {
        return SINGLETON;
    }

    private final SecretKey key;

    /**
     * Creates a new instance with the given AES {@link SecretKey}.
     *
     * <p>Use this constructor when tokens must be decodable across process restarts or
     * multiple JVM instances sharing the same key material.
     *
     * @param key a 128, 192, or 256-bit AES key; must not be {@code null}
     */
    public JdkFenceTokenEncryption(SecretKey key) {
        this.key = key;
    }

    /**
     * Creates a new instance with a freshly generated, cryptographically random 256-bit AES key.
     *
     * <p>Tokens produced by this instance are only decodable by the same instance (same key).
     * This is appropriate for single-JVM, single-process scenarios.
     *
     * @throws IllegalStateException if AES is not available on this JVM (should never happen
     *                               on a standard JDK)
     */
    public JdkFenceTokenEncryption() {
        try {
            KeyGenerator gen = KeyGenerator.getInstance("AES");
            gen.init(256);
            this.key = gen.generateKey();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("AES unavailable", e);
        }
    }

    /**
     * Encrypts {@code data} using AES-256-GCM and returns a URL-safe Base64 string.
     *
     * <p>A fresh 12-byte IV is generated via {@link SecureRandom} for each call. The output
     * format is {@code [IV (12 bytes)][ciphertext + 128-bit GCM auth tag]}, URL-safe Base64
     * encoded without padding.
     *
     * @param data the plaintext bytes; must not be {@code null}
     * @return a URL-safe Base64-encoded ciphertext string
     * @throws IllegalStateException if encryption fails unexpectedly
     */
    @Override
    public String encrypt(byte[] data) {
        try {
            byte[] iv = new byte[IV_LENGTH_BYTES];
            new SecureRandom().nextBytes(iv);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            byte[] ciphertext = cipher.doFinal(data);

            byte[] output = new byte[IV_LENGTH_BYTES + ciphertext.length];
            System.arraycopy(iv, 0, output, 0, IV_LENGTH_BYTES);
            System.arraycopy(ciphertext, 0, output, IV_LENGTH_BYTES, ciphertext.length);

            return Base64.getUrlEncoder().withoutPadding().encodeToString(output);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Encryption failed", e);
        }
    }

    /**
     * Decrypts a string produced by {@link #encrypt} and returns the original plaintext bytes.
     *
     * @param encoded the URL-safe Base64 ciphertext string; must not be {@code null}
     * @return the original plaintext bytes
     * @throws IllegalStateException if decryption fails (wrong key, tampered ciphertext, or
     *                               GCM authentication failure)
     */
    @Override
    public byte[] decrypt(String encoded) {
        try {
            byte[] input = Base64.getUrlDecoder().decode(encoded);
            byte[] iv = new byte[IV_LENGTH_BYTES];
            System.arraycopy(input, 0, iv, 0, IV_LENGTH_BYTES);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            return cipher.doFinal(input, IV_LENGTH_BYTES, input.length - IV_LENGTH_BYTES);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Decryption failed", e);
        }
    }
}
