package io.parsley;

/**
 * SPI for encrypting and decrypting the payload of a {@link FenceToken}.
 *
 * <p>Implementations are discovered at runtime via {@link java.util.ServiceLoader}.
 * Register a custom implementation by adding its fully qualified class name to
 * {@code META-INF/services/io.parsley.FenceTokenEncryption} (classpath mode) or
 * via a {@code provides} declaration in a {@code module-info.java} (module mode).
 *
 * <p>The default implementation shipped with Parsley is
 * {@code io.parsley.crypto.jdk.aes.JdkFenceTokenEncryption} (AES-256-GCM, ephemeral per-JVM key).
 * Replace it when tokens must survive process restarts or cross JVM boundaries.
 */
public interface FenceTokenEncryption {

    /**
     * Encrypts {@code data} and returns a URL-safe Base64-encoded ciphertext string.
     *
     * @param data the plaintext bytes to encrypt; must not be {@code null}
     * @return a non-blank, URL-safe string that can be safely embedded in HTTP headers or
     *         message attributes
     */
    String encrypt(byte[] data);

    /**
     * Decrypts an {@link #encrypt}-ed string and returns the original plaintext bytes.
     *
     * @param encoded the URL-safe ciphertext string produced by {@link #encrypt}
     * @return the original plaintext bytes
     * @throws IllegalStateException if decryption fails (e.g. wrong key, tampered input)
     */
    byte[] decrypt(String encoded);
}
