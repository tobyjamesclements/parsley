package io.github.tobyjamesclements.parsley.session;

import java.util.List;
import java.util.OptionalLong;
import java.util.TreeMap;

import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.Deliverability;

/**
 * A causal past carried at the edge of the pipeline: a session token held by a client, or
 * the recorded past stored beside a projected row.
 *
 * <p>Both are the same object — a frontier of {@code (channel, position)} pairs — used from
 * two sides of one comparison: {@link #coverageOf(CausalPast)} answers whether the past
 * recorded against served data accounts for everything a client's token names. A write tier
 * stamps a validated token onto produced messages as causes; a read tier refuses to serve
 * data whose recorded past does not cover the token, and refreshes the token by merging in
 * what it served. That buys session consistency — read-your-writes, monotonic reads,
 * writes-follow-reads — for participants outside the delivery protocol (issue #96).
 *
 * <p>The wire form is the frozen causes grammar itself ({@link CausesCodec}), so
 * {@link #decode(byte[])} is exactly as strict as the engine's own header decode: a token
 * that is truncated, miscounted, padded or out of canonical order is refused, never
 * salvaged. A salvaging token parser is the same hazard as a salvaging codec — a weaker
 * frontier read from damaged bytes silently weakens the session guarantee.
 *
 * <p>Coverage deliberately inverts the delivery gate's disposition towards unknown
 * channels. {@link Deliverability#decide} skips a cause on a channel outside the received
 * set, because a gate that waited for what it will never see would wait forever. A read
 * tier is not a gate: a channel its recorded past cannot verify must mean <em>do not
 * serve</em>, not serve anyway. Every channel a token names is therefore checked, and a
 * channel the past has never recorded is reported as a {@linkplain Coverage#gaps() gap}.
 *
 * <p>Instances are immutable; {@link #merge(ChannelId, long)} and {@link #merge(CausalPast)}
 * return new pasts. The engine never reads this type: it is a companion over the public
 * surface, not part of the delivery protocol, and holding one grants no delivery guarantee.
 *
 * @see Causes
 * @see CausesCodec
 * @see Deliverability
 */
public final class CausalPast {
    private static final CausalPast NONE = new CausalPast(Causes.none());

    private final Causes causes;

    private CausalPast(Causes causes) {
        this.causes = causes;
    }

    /**
     * The empty past: a session that has observed nothing.
     *
     * @return a past naming no channel
     */
    public static CausalPast none() {
        return NONE;
    }

    /**
     * Builds a past over an existing frontier.
     *
     * @param causes the frontier to carry
     * @return the past
     * @throws IllegalArgumentException if {@code causes} is null
     */
    public static CausalPast of(Causes causes) {
        if (causes == null) {
            throw new IllegalArgumentException("causes must be non-null");
        }
        return causes.isEmpty() ? NONE : new CausalPast(causes);
    }

    /**
     * Decodes a past from the frozen wire grammar, exactly as strictly as the engine
     * decodes a {@code parsley.causes} header.
     *
     * @param encoded the encoded past, as {@link #encode()} produced it or as read from a
     *                record's {@link CausesCodec#HEADER_KEY} header
     * @return the past
     * @throws CausesCodec.UndecodableMetadataException if the bytes cannot be trusted, for
     *         any of the reasons {@link CausesCodec#decode(byte[])} names; a token that
     *         cannot be decoded must be treated as no token, never as a partial one
     */
    public static CausalPast decode(byte[] encoded) throws CausesCodec.UndecodableMetadataException {
        return of(CausesCodec.decode(encoded));
    }

    /**
     * Encodes this past in the frozen wire grammar.
     *
     * <p>The bytes are canonical — one past, one spelling — and are valid as a
     * {@code parsley.causes} header value, which is what lets a write tier stamp a
     * validated token straight onto a produced record.
     *
     * @return the encoded past
     */
    public byte[] encode() {
        return CausesCodec.encode(causes);
    }

    /**
     * Returns the frontier this past carries.
     *
     * @return the frontier this past carries
     */
    public Causes causes() {
        return causes;
    }

    /**
     * Returns {@code true} when this past names no channel.
     *
     * @return {@code true} when this past names no channel
     */
    public boolean isEmpty() {
        return causes.isEmpty();
    }

    /**
     * Returns how many channels this past names.
     *
     * <p>Bound this, and the {@link #encode() encoded} width, before trusting an inbound
     * token: a token is untrusted input even when this application minted it.
     *
     * @return how many channels this past names
     */
    public int size() {
        return causes.size();
    }

    /**
     * Merges one observed coordinate: the delivered message's own {@code (channel,
     * position)} at a projector's seam, or the coordinate a produce acknowledgement
     * confirmed at a write tier.
     *
     * @param channel  the channel observed
     * @param position the position observed on it
     * @return a past carrying the greater of {@code position} and any position already
     *         named for {@code channel}; this instance is unchanged, and is returned
     *         itself when it already covers the coordinate
     * @throws IllegalArgumentException if {@code channel} is null or {@code position} is
     *                                  negative
     */
    public CausalPast merge(ChannelId channel, long position) {
        if (channel == null) {
            throw new IllegalArgumentException("channel must be non-null");
        }
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative on " + channel + ": " + position);
        }
        Long current = causes.byChannel().get(channel);
        if (current != null && current >= position) {
            return this;
        }
        TreeMap<ChannelId, Long> merged = new TreeMap<>(causes.byChannel());
        merged.put(channel, position);
        return new CausalPast(Causes.of(merged));
    }

    /**
     * Merges another past pointwise: per channel, the greater position wins. This is how a
     * read tier folds the served data's recorded past into the client's token before
     * re-minting it.
     *
     * @param other the past to merge in
     * @return a past covering both; this instance is unchanged, and when either side is
     *         empty the other is returned itself
     * @throws IllegalArgumentException if {@code other} is null
     */
    public CausalPast merge(CausalPast other) {
        if (other == null) {
            throw new IllegalArgumentException("other must be non-null");
        }
        if (other.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return other;
        }
        TreeMap<ChannelId, Long> merged = new TreeMap<>(causes.byChannel());
        other.causes.byChannel().forEach((channel, position) -> merged.merge(channel, position, Long::max));
        return new CausalPast(Causes.of(merged));
    }

    /**
     * Decides whether this past covers a token: whether everything the token names is at or
     * below what this past records.
     *
     * <p>The check reuses the core's decision ({@link Deliverability#decide}) with the
     * token's own channel set as the received set, so no channel is skippable: a channel
     * this past has never recorded fails the check with an
     * {@linkplain OptionalLong#empty() empty} recorded position, where the delivery gate
     * would have skipped it. Serving on an unverifiable channel is exactly the
     * read-your-writes violation this type exists to prevent, so the error here is always
     * in the conservative direction — a refusal to serve, never a stale serve.
     *
     * @param token the past that must be accounted for, typically a client's session token
     * @return the verdict, naming every gap when this past falls short
     * @throws IllegalArgumentException if {@code token} is null
     */
    public Coverage coverageOf(CausalPast token) {
        if (token == null) {
            throw new IllegalArgumentException("token must be non-null");
        }
        Deliverability.Verdict verdict = Deliverability.decide(
                token.causes,
                token.causes.byChannel().keySet(),
                channel -> {
                    Long recorded = causes.byChannel().get(channel);
                    return recorded == null ? OptionalLong.empty() : OptionalLong.of(recorded);
                });
        return new Coverage(verdict instanceof Deliverability.Held held ? held.blockers() : List.of());
    }

    /**
     * The outcome of one coverage check.
     *
     * @param gaps every token entry this past does not account for, in {@link ChannelId}
     *             order; each names the channel, the position the token requires and the
     *             position this past records for it, empty when it records none
     */
    public record Coverage(List<Deliverability.Blocker> gaps) {
        /**
         * Copies the gaps.
         *
         * @throws IllegalArgumentException if {@code gaps} is null
         */
        public Coverage {
            if (gaps == null) {
                throw new IllegalArgumentException("gaps must be non-null");
            }
            gaps = List.copyOf(gaps);
        }

        /**
         * Returns {@code true} when the past accounts for everything the token names.
         *
         * @return {@code true} when the past accounts for everything the token names
         */
        public boolean covers() {
            return gaps.isEmpty();
        }
    }

    /**
     * @param o the object to compare against
     * @return {@code true} when {@code o} is a past carrying an equal frontier
     */
    @Override
    public boolean equals(Object o) {
        return o instanceof CausalPast other && causes.equals(other.causes);
    }

    /**
     * Returns a hash consistent with {@link #equals(Object)}.
     *
     * @return a hash consistent with {@link #equals(Object)}
     */
    @Override
    public int hashCode() {
        return causes.hashCode();
    }

    /**
     * Returns the past rendered for diagnostics.
     *
     * @return the past rendered for diagnostics
     */
    @Override
    public String toString() {
        return "CausalPast" + causes.byChannel();
    }
}
