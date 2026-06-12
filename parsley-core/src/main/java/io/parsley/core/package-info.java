/**
 * The Parsley engine: broker-neutral causal buffering, fence-token creation, and SPI
 * resolution.
 *
 * <h2>Key types</h2>
 * <ul>
 *   <li>{@link io.parsley.core.CausalEngine} &mdash; the buffering engine broker adapters
 *       drive: classify, buffer, advance the frontier, cascade releases</li>
 *   <li>{@link io.parsley.core.CausalBuffers} &mdash; factory for the standard
 *       {@link io.parsley.CausalBuffer} implementations (ignore, drop, dead-letter)</li>
 *   <li>{@link io.parsley.core.FenceTokens} &mdash; creates and decodes
 *       {@link io.parsley.FenceToken}s</li>
 *   <li>{@link io.parsley.core.ParsleyServices} &mdash; deterministic SPI resolution,
 *       configurable via {@code parsley.properties}</li>
 * </ul>
 */
package io.parsley.core;
