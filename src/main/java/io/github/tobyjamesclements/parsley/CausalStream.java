package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One or more causal source topics, ready to feed a single causal-decorated processor with
 * {@link #process}. Returned by {@link CausalStreamsBuilder#stream}; combine several with {@link #merge}
 * to fan topics declared with different serdes into one stage (mirrors
 * {@link org.apache.kafka.streams.kstream.KStream#merge}).
 *
 * @param <K> the source key type
 * @param <V> the source value type
 */
public final class CausalStream<K, V> {

    private final CausalStreamsBuilder owner;
    private final Map<String, ParsleyStageSpec.SourceSpec<K, V>> sources;

    CausalStream(CausalStreamsBuilder owner, Map<String, ParsleyStageSpec.SourceSpec<K, V>> sources) {
        this.owner = owner;
        this.sources = sources;
    }

    /**
     * Combines this stream's source topics with {@code other}'s into one, so a later {@link #process}
     * call fans every topic from both into the same causal stage. Use this to combine topics registered
     * with different serdes — each {@link CausalStreamsBuilder#stream} call carries one serde pair, so
     * heterogeneous-serde fan-in is a {@code stream(...)} per serde group followed by {@code merge}.
     *
     * @param other another stream from the same {@link CausalStreamsBuilder}
     * @return a new {@code CausalStream} over the union of both streams' source topics
     * @throws IllegalArgumentException if {@code other} was not built from the same builder, or if the
     *         two streams declare the same topic (whose serdes would otherwise silently collide,
     *         last-write-wins)
     */
    public CausalStream<K, V> merge(CausalStream<K, V> other) {
        if (other.owner != owner) {
            throw new IllegalArgumentException(
                    "cannot merge a CausalStream built from a different CausalStreamsBuilder");
        }
        Map<String, ParsleyStageSpec.SourceSpec<K, V>> merged = new LinkedHashMap<>(sources);
        for (Map.Entry<String, ParsleyStageSpec.SourceSpec<K, V>> entry : other.sources.entrySet()) {
            if (merged.putIfAbsent(entry.getKey(), entry.getValue()) != null) {
                throw new IllegalArgumentException("both streams declare topic '" + entry.getKey()
                        + "'; declare each source topic exactly once — merging would silently pick one "
                        + "stream's serdes over the other's");
            }
        }
        return new CausalStream<>(owner, merged);
    }

    /**
     * Binds this stream's source topics to one causal-decorated processor. The stage's name — the causal
     * buffer store's namespace, hence its changelog topics — is auto-derived from the runtime's
     * {@code application.id} and this builder's declaration order; use
     * {@link #process(String, ProcessorSupplier)} to name it explicitly instead.
     *
     * @param supplier the user's processor supplier (its declared state stores are unioned with
     *                 Parsley's internal frontier and buffer stores)
     * @param <KOut>   the forwarded key type
     * @param <VOut>   the forwarded value type
     * @return a {@link CausalProcessedStream} to declare this stage's sink(s) on
     */
    public <KOut, VOut> CausalProcessedStream<KOut, VOut> process(ProcessorSupplier<K, V, KOut, VOut> supplier) {
        ParsleyStageSpec<K, V, KOut, VOut> spec = new ParsleyStageSpec<>(null, sources, supplier);
        owner.registerStage(spec);
        return new CausalProcessedStream<>(owner, spec);
    }

    /**
     * As {@link #process(ProcessorSupplier)}, naming the stage explicitly — the name becomes the causal
     * buffer store's namespace, so give it a stable, unique name across topology edits and restarts
     * rather than relying on the auto-derived default.
     *
     * @param name     this stage's name
     * @param supplier the user's processor supplier
     * @param <KOut>   the forwarded key type
     * @param <VOut>   the forwarded value type
     * @return a {@link CausalProcessedStream} to declare this stage's sink(s) on
     */
    public <KOut, VOut> CausalProcessedStream<KOut, VOut> process(
            String name, ProcessorSupplier<K, V, KOut, VOut> supplier) {
        Objects.requireNonNull(name, "name must not be null");
        ParsleyStageSpec<K, V, KOut, VOut> spec = new ParsleyStageSpec<>(name, sources, supplier);
        owner.registerStage(spec);
        return new CausalProcessedStream<>(owner, spec);
    }
}
