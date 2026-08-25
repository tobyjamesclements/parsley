/**
 * Session consistency at the edge: the companion surface for carrying a causal past beyond
 * the last consumer.
 *
 * <p>The delivery protocol ends at a process's seam. This package extends its frontier to
 * participants outside the protocol — an HTTP gateway minting session tokens, a read tier
 * refusing to serve data that does not yet reflect a client's writes, a projector recording
 * beside each row the past that row reflects. One type,
 * {@link io.github.tobyjamesclements.parsley.session.CausalPast}, plays every role: parse,
 * merge, encode, and the coverage check.
 *
 * <p>Everything here rides the core's public surface — the frozen wire grammar, the
 * frontier value and the pure decision — and none of it is read by the engine. It is an
 * application-layer pattern over Parsley, not a change to the guarantee Parsley provides:
 * holding a past grants no delivery guarantee, and a read model that wants its rows ordered
 * still needs to be a Parsley process (issue #96).
 *
 * @see io.github.tobyjamesclements.parsley.session.CausalPast
 */
package io.github.tobyjamesclements.parsley.session;
