/**
 * The declaration surface: what an application writes.
 *
 * <p>An application declares {@link io.github.tobyjamesclements.parsley.api.Channel} typed
 * topics, {@link io.github.tobyjamesclements.parsley.api.StoreDef} typed stores, and one
 * {@link io.github.tobyjamesclements.parsley.api.ProcessDefinition} per process. Logic is a
 * {@link io.github.tobyjamesclements.parsley.api.Handler}, which receives a
 * {@link io.github.tobyjamesclements.parsley.api.Delivery} and returns
 * {@link io.github.tobyjamesclements.parsley.api.Effects}.
 *
 * <p>A handler is given no producer, no timer and no clock. Everything it changes travels
 * back through its return value, which is what allows state, sends and read positions to
 * commit in one transaction.
 *
 * @see io.github.tobyjamesclements.parsley.api.Parsley#start
 */
package io.github.tobyjamesclements.parsley.api;
