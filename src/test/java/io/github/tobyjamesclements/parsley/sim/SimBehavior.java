package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.Delivery;

/**
 * The user processor a simulated node runs. Returns the number of business records it
 * forwarded (the host uses it to drive null-message emission).
 */
interface SimBehavior {

    int process(SimNode host, Delivery delivery);

    /** Forwards every delivery to each sink, keyed and timestamped as delivered. */
    static SimBehavior forwardTo(String... topics) {
        return (host, d) -> {
            for (String t : topics) host.sendBusiness(t, d.key(), d.value(), d.timestamp());
            return topics.length;
        };
    }

    /** Forwards only records whose value hash is divisible by {@code mod}; drops the rest. */
    static SimBehavior filter(int mod, String... topics) {
        return (host, d) -> {
            int h = d.value() == null ? 0 : Math.floorMod(java.util.Arrays.hashCode(d.value()), mod);
            if (h != 0) return 0;
            for (String t : topics) host.sendBusiness(t, d.key(), d.value(), d.timestamp());
            return topics.length;
        };
    }

    /** Consumes without ever forwarding (a pure sink / auditing stage). */
    static SimBehavior consumeOnly() {
        return (host, d) -> 0;
    }
}
