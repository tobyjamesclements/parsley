package io.parsley.core;

public final class BetaGreeting implements GreetingSpi {
    @Override
    public String greet() {
        return "beta";
    }
}
