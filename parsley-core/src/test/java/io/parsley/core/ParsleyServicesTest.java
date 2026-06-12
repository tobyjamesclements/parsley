package io.parsley.core;

import io.parsley.FenceTokenEncryption;
import io.parsley.VectorClockSerialiser;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ParsleyServicesTest {

    private static ParsleyServices services(String... keyValues) {
        Properties properties = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            properties.setProperty(keyValues[i], keyValues[i + 1]);
        }
        return ParsleyServices.from(properties);
    }

    @Test
    void undeclaredWithSingleProviderResolvesIt() {
        VectorClockSerialiser<?> serialiser = services().resolve(VectorClockSerialiser.class);
        assertInstanceOf(TestClockSerialiser.class, serialiser);
    }

    @Test
    void undeclaredWithMultipleProvidersFails() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> services().resolve(GreetingSpi.class));
        assertTrue(ex.getMessage().contains("AlphaGreeting"));
        assertTrue(ex.getMessage().contains("BetaGreeting"));
        assertTrue(ex.getMessage().contains("parsley.properties"));
    }

    @Test
    void undeclaredWithNoProviderFails() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> services().resolve(UnprovidedSpi.class));
        assertTrue(ex.getMessage().contains(UnprovidedSpi.class.getName()));
    }

    @Test
    void declaredSelectsAmongMultipleProviders() {
        GreetingSpi greeting = services(
                GreetingSpi.class.getName(), BetaGreeting.class.getName())
                .resolve(GreetingSpi.class);
        assertEquals("beta", greeting.greet());
    }

    @Test
    void declaredButNotRegisteredFails() {
        ParsleyServices declared = services(GreetingSpi.class.getName(), "com.example.Missing");
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> declared.resolve(GreetingSpi.class));
        assertTrue(ex.getMessage().contains("com.example.Missing"));
        assertTrue(ex.getMessage().contains("AlphaGreeting"));
    }

    @Test
    void resolveCachesTheInstance() {
        ParsleyServices resolver = services();
        assertSame(resolver.resolve(FenceTokenEncryption.class),
                resolver.resolve(FenceTokenEncryption.class));
    }

    @Test
    void separateResolversAreIndependent() {
        GreetingSpi alpha = services(GreetingSpi.class.getName(), AlphaGreeting.class.getName())
                .resolve(GreetingSpi.class);
        GreetingSpi beta = services(GreetingSpi.class.getName(), BetaGreeting.class.getName())
                .resolve(GreetingSpi.class);
        assertEquals("alpha", alpha.greet());
        assertEquals("beta", beta.greet());
    }

    @Test
    void globalResolvesSingleProviders() {
        assertInstanceOf(TestEncryption.class,
                ParsleyServices.global().resolve(FenceTokenEncryption.class));
        assertSame(ParsleyServices.global(), ParsleyServices.global());
    }

    @Test
    void loadPropertiesReadsClasspathFile() {
        // Direct test of the file-loading seam: Holder initialises global() only once
        // per JVM, so mutation coverage of these lines needs a per-test entry point.
        Properties loaded = ParsleyServices.loadProperties();
        assertEquals("io.parsley.core.AlphaGreeting",
                loaded.getProperty("io.parsley.core.GreetingSpi"));
    }

    @Test
    void globalReadsParsleyPropertiesFromClasspath() {
        // src/test/resources/parsley.properties declares AlphaGreeting for GreetingSpi,
        // which has two registered providers — resolution succeeds only if the file
        // was actually loaded and the declaration applied.
        GreetingSpi greeting = ParsleyServices.global().resolve(GreetingSpi.class);
        assertEquals("alpha", greeting.greet());
    }
}
