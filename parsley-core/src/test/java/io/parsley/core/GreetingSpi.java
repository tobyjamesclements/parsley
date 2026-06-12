package io.parsley.core;

/** Test SPI with two registered providers, for exercising ambiguous resolution. */
public interface GreetingSpi {
    String greet();
}
