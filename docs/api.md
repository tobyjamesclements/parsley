# API

The generated Javadoc for this version is published alongside this site.

[Browse the Javadoc](api/index.html)

## Packages

`io.github.tobyjamesclements.parsley.api` is the declaration surface. An application declares
`Channel` typed topics, `StoreDef` typed stores, and one `ProcessDefinition` per process.
Logic is a `Handler`, receiving a `Delivery` and returning `Effects`.

`io.github.tobyjamesclements.parsley.core` is the protocol, independent of any host. It names
no host type and touches nothing outside its `OrderingStore`. The safety rule is
`Deliverability.decide`, a pure function. `ProcessEngine` holds the state that function reads.

`io.github.tobyjamesclements.parsley.kafka` is the Kafka Streams adapter.

## Automatic module name

`io.github.tobyjamesclements.parsley`
