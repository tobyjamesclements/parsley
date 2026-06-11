/**
 * JDK AES-256-GCM implementation of {@link io.parsley.FenceTokenEncryption}.
 *
 * <p>Registered automatically as the default provider via {@link java.util.ServiceLoader}.
 * Uses only standard {@code javax.crypto} APIs &mdash; no third-party cryptography
 * library is required.
 *
 * <p>The default (no-arg) constructor generates an ephemeral key per JVM instance. To share
 * fence tokens across JVM restarts or between services, supply a persistent
 * {@link javax.crypto.SecretKey} via the single-argument constructor.
 */
package io.parsley.crypto.jdk.aes;
