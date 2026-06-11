/**
 * JDK AES-256-GCM implementation of {@link io.parsley.FenceTokenEncryption}.
 *
 * <p>Provides {@link io.parsley.crypto.jdk.JdkFenceTokenEncryption}, which is registered
 * automatically as the default {@link io.parsley.FenceTokenEncryption} via
 * {@link java.util.ServiceLoader}. Uses only standard {@code javax.crypto} APIs; no
 * third-party cryptography libraries are required.
 *
 * <p>By default an ephemeral AES-256-GCM key is generated per JVM instance, making fence
 * tokens valid only within that JVM. Inject a persistent {@link javax.crypto.SecretKey} via
 * the single-argument constructor to share tokens across restarts or processes.
 */
module io.parsley.crypto.jdk {
    requires io.parsley;
    exports io.parsley.crypto.jdk;
    provides io.parsley.FenceTokenEncryption
            with io.parsley.crypto.jdk.JdkFenceTokenEncryption;
}
