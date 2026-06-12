/**
 * The Parsley engine: broker-neutral causal buffering, fence-token creation, and SPI
 * resolution.
 *
 * <p>This module depends only on the {@code io.parsley} API module. Broker adapters
 * (e.g. {@code parsley-kafka}) build on this engine; SPI implementors depend only on the
 * API module.
 */
module io.parsley.core {
    requires transitive io.parsley;
    exports io.parsley.core;
    uses io.parsley.FenceTokenEncryption;
    uses io.parsley.VectorClockSerialiser;
}
