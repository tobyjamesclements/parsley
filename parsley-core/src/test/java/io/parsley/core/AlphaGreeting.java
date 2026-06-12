package io.parsley.core;

public final class AlphaGreeting implements GreetingSpi {
    @Override
    public String greet() {
        return "alpha";
    }
}
