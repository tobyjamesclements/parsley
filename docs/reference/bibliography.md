# Bibliography

The sources Parsley's design and naming draw on. Pages across [Foundations](../foundations/causal-consistency.md)
and [The three protocols](../protocols/index.md) cite these as footnotes, and the
[naming register](naming.md) records which term each identifier adopts, adapts, or declines.

## Causal order and vector clocks

- **Lamport, 1978.** Leslie Lamport, "Time, Clocks, and the Ordering of Events in a Distributed
  System", *Communications of the ACM* 21(7). Defines the happened-before relation Parsley's
  guarantee is stated over.
- **Fidge, 1988.** Colin J. Fidge, "Timestamps in Message-Passing Systems That Preserve the Partial
  Ordering", *Proceedings of the 11th Australian Computer Science Conference*.
- **Mattern, 1988.** Friedemann Mattern, "Virtual Time and Global States of Distributed Systems",
  *Proceedings of the International Workshop on Parallel and Distributed Algorithms*. With Fidge,
  the vector-clock construction; the source of Parsley's `frontier` and `completeness` (VT(p))
  vocabulary.
- **Schwarz and Mattern, 1994.** Reinhard Schwarz and Friedemann Mattern, "Detecting Causal
  Relationships in Distributed Computations: In Search of the Holy Grail", *Distributed Computing*
  7(3). Causal histories and the VT(m) message-timestamp role; the ground-truth model
  `ParsleyTopologySim` follows.

## Causal broadcast and multicast

- **Birman, Schiper, and Stephenson, 1991.** Kenneth Birman, André Schiper, and Pat Stephenson,
  "Lightweight Causal and Atomic Group Multicast", *ACM Transactions on Computer Systems* 9(3). The
  ISIS system's CBCAST protocol, whose delivery condition is Parsley's
  [delivery gate](../foundations/delivery-gate.md).
- **Hadzilacos and Toueg, 1994.** Vassos Hadzilacos and Sam Toueg, "A Modular Approach to
  Fault-Tolerant Broadcasts and Related Problems", Cornell University technical report. The
  reliable-channel abstraction the [channels module](../protocols/channels.md) adapts Kafka
  topic-partitions into.
- **Cachin, Guerraoui, and Rodrigues, 2011.** Christian Cachin, Rachid Guerraoui, and Luís
  Rodrigues, *Introduction to Reliable and Secure Distributed Programming*, 2nd edition, Springer.
  The module presentation style (requests, indications, properties) the three protocol pages use.

## Distributed simulation and null messages

- **Bryant, 1977.** Randal E. Bryant, "Simulation of Packet Communication Architecture Computer
  Systems", MIT Laboratory for Computer Science.
- **Chandy and Misra, 1979.** K. Mani Chandy and Jayadev Misra, "Distributed Simulation: A Case
  Study in Design and Verification of Distributed Programs", *IEEE Transactions on Software
  Engineering*. With Bryant, the null-message protocol whose trigger discipline the
  [gossip module](../protocols/gossip.md) relay rule follows.
- **DeVries, 1990.** Raymond C. DeVries, "Reducing Null Messages in Misra's Distributed
  Discrete-Event Simulation Method", *IEEE Transactions on Software Engineering*. Precedent for
  reducing null-message volume.
- **Cai and Turner, 1990; Wood and Turner, 1994.** Precedents for the cycle-echo problem in
  conservative distributed simulation (Wood and Turner, *Proceedings of PADS '94*), which the relay
  rule's consumed-channel restriction addresses.

## Epidemic dissemination

- **Demers et al., 1987.** Alan Demers, Dan Greene, Carl Hauser, Wes Irish, John Larson, Scott
  Shenker, Howard Sturgis, Dan Swinehart, and Doug Terry, "Epidemic Algorithms for Replicated
  Database Maintenance", *Proceedings of PODC*. The epidemic-dissemination sense in which the gossip
  module advertises clock progress.

## Matrix clocks

- **Wuu and Bernstein, 1984.** Gene T. J. Wuu and Arthur J. Bernstein, "Efficient Solutions to the
  Replicated Log and Dictionary Problems", *Proceedings of PODC*.
- **Sarin and Lynch, 1987.** Sunil K. Sarin and Nancy A. Lynch, "Discarding Obsolete Information in a
  Replicated Database System", *IEEE Transactions on Software Engineering*. The matrix-clock lineage
  the per-channel advertised clocks are a row of.

## Causal consistency for storage

- **Lloyd, Freedman, Kaminsky, and Andersen, 2011.** Wyatt Lloyd, Michael J. Freedman, Michael
  Kaminsky, and David G. Andersen, "Don't Settle for Eventual: Scalable Causal Consistency for
  Wide-Area Storage with COPS", *Proceedings of SOSP*. The total-visibility form of the assumption
  Parsley's [delivery gate](../foundations/delivery-gate.md) removes.
