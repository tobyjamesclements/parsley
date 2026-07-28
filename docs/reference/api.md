# API reference

The Javadoc for this version of Parsley is built from the same sources these pages describe
and is published alongside them. Each version in the site's version selector carries the API
reference built from its own sources.

<a href="../../api/" class="md-button">Browse the Javadoc</a>

The API is one package with two tiers, and the package overview in the Javadoc codifies
them. The functional core is the vocabulary user logic is written in: `Topic`, `Codec`,
`Message`, `Emission`, `Handler`, `Fold`, and `Step`. The imperative edges run that logic:
`Stage` wires it to topics, `Parsley` composes stages, `CausalStreams` is the running
application, and `CausalClock` with `CausalHeaders` serve plain clients at the application's
borders. Everything else is package-private, because the protocol is only sound under a host
contract no API can enforce, and the one host that upholds that contract ships inside the
package.
