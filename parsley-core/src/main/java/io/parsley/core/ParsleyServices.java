package io.parsley.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves SPI implementations deterministically.
 *
 * <p>Implementations are registered via {@link ServiceLoader} (a
 * {@code META-INF/services} file or a module {@code provides} declaration) and selected
 * by the following rules, evaluated per SPI interface:
 *
 * <ol>
 *   <li>If a {@code parsley.properties} file at the classpath root declares an
 *       implementation — key: the SPI interface's fully qualified name, value: the
 *       implementation class's fully qualified name — that implementation is used.
 *       If the declared class is not among the registered providers, resolution fails.
 *   <li>If nothing is declared and exactly one provider is registered, it is used.
 *   <li>If nothing is declared and zero or multiple providers are registered,
 *       resolution fails with a message naming the candidates.
 * </ol>
 *
 * <p>Resolved instances are cached per {@code ParsleyServices} instance, so repeated
 * {@link #resolve} calls return the same object — e.g. all fence tokens in a JVM share one
 * {@link io.parsley.FenceTokenEncryption} instance (and therefore one ephemeral key).
 *
 * <p>Use {@link #global()} in production code; {@link #from(Properties)} exists for tests
 * and for hosts that manage configuration themselves.
 *
 * <p><strong>Modular applications:</strong> {@code parsley.properties} is loaded via the
 * thread context class loader; an application running fully on the module path must place
 * the file where that loader can see it (for classpath deployments — the common case — the
 * resource root suffices). Module-path provider resolution (including providers exposed via
 * a static {@code provider()} factory method) follows the same selection rules, but the
 * test suite exercises resolution in classpath mode only — fully-modular deployments
 * should verify SPI resolution at application startup.
 */
public final class ParsleyServices {

    /** The classpath resource consulted by {@link #global()}. */
    public static final String PROPERTIES_RESOURCE = "parsley.properties";

    private final Properties properties;
    private final ConcurrentHashMap<Class<?>, Object> cache = new ConcurrentHashMap<>();

    private ParsleyServices(Properties properties) {
        this.properties = properties;
    }

    /**
     * Returns the JVM-wide resolver, configured from {@code parsley.properties} at the
     * classpath root (or empty configuration if the file is absent). Loaded lazily on
     * first use.
     *
     * @return the shared resolver
     */
    public static ParsleyServices global() {
        return Holder.GLOBAL;
    }

    /**
     * Returns a resolver configured from the given properties instead of
     * {@code parsley.properties}. Intended for tests and embedding hosts.
     *
     * @param properties the SPI declarations; copied defensively
     * @return a new, independent resolver
     */
    public static ParsleyServices from(Properties properties) {
        Properties copy = new Properties();
        copy.putAll(properties);
        return new ParsleyServices(copy);
    }

    /**
     * Resolves the implementation of {@code spi} according to the class-level rules.
     *
     * @param <S> the SPI type
     * @param spi the SPI interface to resolve
     * @return the resolved implementation; the same instance on every call
     * @throws IllegalStateException if resolution fails (declared class not registered,
     *                               no provider, or ambiguous providers)
     */
    public <S> S resolve(Class<S> spi) {
        return spi.cast(cache.computeIfAbsent(spi, this::load));
    }

    private <S> S load(Class<S> spi) {
        List<ServiceLoader.Provider<S>> providers = ServiceLoader.load(spi).stream().toList();
        String declared = properties.getProperty(spi.getName());
        if (declared != null) {
            return providers.stream()
                    .filter(provider -> provider.type().getName().equals(declared))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            PROPERTIES_RESOURCE + " declares " + declared + " for " + spi.getName()
                                    + " but no such provider is registered. Registered providers: "
                                    + names(providers)))
                    .get();
        }
        if (providers.isEmpty()) {
            throw new IllegalStateException(
                    "No " + spi.getName() + " provider is registered. Add a module that provides one"
                            + " (e.g. parsley-kafka for VectorClockSerialiser, parsley-crypto-jdk-aes for"
                            + " FenceTokenEncryption) or register your own via META-INF/services.");
        }
        if (providers.size() > 1) {
            throw new IllegalStateException(
                    "Multiple " + spi.getName() + " providers are registered: " + names(providers)
                            + ". Select one in " + PROPERTIES_RESOURCE + ": "
                            + spi.getName() + "=<implementation class>");
        }
        return providers.getFirst().get();
    }

    private static String names(List<? extends ServiceLoader.Provider<?>> providers) {
        return providers.stream()
                .map(provider -> provider.type().getName())
                .sorted()
                .toList()
                .toString();
    }

    private static final class Holder {
        static final ParsleyServices GLOBAL = new ParsleyServices(loadProperties());
    }

    /** Package-private for direct testing — the {@link Holder} initialises once per JVM. */
    static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream in = openPropertiesResource()) {
            if (in != null) {
                properties.load(in);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read " + PROPERTIES_RESOURCE, e);
        }
        return properties;
    }

    private static InputStream openPropertiesResource() {
        ClassLoader context = Thread.currentThread().getContextClassLoader();
        if (context != null) {
            InputStream in = context.getResourceAsStream(PROPERTIES_RESOURCE);
            if (in != null) {
                return in;
            }
        }
        return ParsleyServices.class.getClassLoader().getResourceAsStream(PROPERTIES_RESOURCE);
    }
}
