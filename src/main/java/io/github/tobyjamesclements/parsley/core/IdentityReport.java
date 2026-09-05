package io.github.tobyjamesclements.parsley.core;

import java.util.Set;

/**
 * What a host learned of channel identity when a process initialised: which channels' topics
 * no longer exist, and which exist again under a new identity.
 *
 * <p>This is the one thing the substrate is asked between deliveries, and it is asked once,
 * at initialisation, rather than on a cadence: a cause names the position of a message that
 * was sent (wire-format constraint 8), so receiving that message is what satisfies it, and
 * nothing about positions needs reporting. Identity is different. A topic that no longer
 * exists can never yield the message a cause names, so its causes can no longer matter
 * (SPEC Structural 13) and its entries leave the frontier; a received topic recreated under
 * its name is a different channel, whose records must not be fed under the old identity
 * (SPEC Assumption 2). Both are learned by asking the substrate, and a host asks when it
 * initialises a process (D115).
 *
 * @param deadChannels      channels whose topic no longer exists
 * @param recreatedChannels channels whose topic exists under a new identity
 */
public record IdentityReport(Set<ChannelId> deadChannels, Set<ChannelId> recreatedChannels) {

    /** Nothing gone and nothing recreated: every channel still is what it was. */
    public static final IdentityReport NONE = new IdentityReport(Set.of(), Set.of());

    /** Defensively copies both sets. */
    public IdentityReport {
        deadChannels = Set.copyOf(deadChannels);
        recreatedChannels = Set.copyOf(recreatedChannels);
    }
}
