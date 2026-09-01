# API

The generated Javadoc for this version is published alongside this site.

[Browse the Javadoc](javadoc/index.html)

## Packages

`io.github.tobyjamesclements.parsley.api` is the declaration surface. An application declares
`Channel` typed topics, `Store` typed stores, and one `ProcessDefinition` per process.
Logic is a `Handler`, receiving a `Delivery` and returning `Effects`.

`io.github.tobyjamesclements.parsley.core` is the protocol, independent of any host. It names
no host type and touches nothing outside its `OrderingStore`. The safety rule is
`Deliverability.decide`, a pure function. `ProcessEngine` holds the state that function reads.

`io.github.tobyjamesclements.parsley.kafka` is the Kafka Streams adapter.

`io.github.tobyjamesclements.parsley.session` is the companion surface for
[session consistency](session.md) at the pipeline's edge: `CausalPast`, a causal frontier
carried as a client token or recorded beside projected data. It rides the core's public
surface and is read by nothing in the other three packages.

## Automatic module name

`io.github.tobyjamesclements.parsley`
