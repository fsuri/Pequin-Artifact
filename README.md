# Basil, TAPIR, TxHotstuff and TxBFTSmart

This repository includes the prototype code used for "Basil: Breaking up BFT with ACID (transactions)" as well as "TAPIR -- the Transaction Application Protocol for Inconsistent Replication." 

TAPIR is a protocol for linearizable distributed transactions built using replication with no consistency guarantees. By enforcing consistency only at the transaction layer, TAPIR eliminates coordination at the replication layer, enabling TAPIR to provide the same transaction model and consistency guarantees as existing systems, like Spanner, with better latency and throughput.
More information on TAPIR can be found here: https://github.com/UWSysLab/tapir.

Basil is a Byzantine Fault Tolerant system that implements distributed and interactive transactions, that like TAPIR does not rely on strong consistency at the replication level. Basil allows transactions to commit across shards in just a single round trip in the common case, and at most two under failure. Transaction processing in Basil is client-driven, and independent of other concurrent but non-conflicting transactions. This combination of low latency and parallelism allows Basil to scale beyond transactional systems built atop strongly consistent BFT SMR protocols. 

Additionally, this repository includes prototype code implementing two transactional key-value stores -- "TxHotstuff" and "TxBFTSmart" -- that serve as BFT baseline systems to compare against Basil. TxHotstuff implements distributed transactions and atomic commit atop libhotstuff, an open source BFT state machine replication library implementing the Hotstuff protocol. More information on Hotstuff can be found here: https://github.com/hot-stuff/libhotstuff
TxBFTSmart implements distributed transactions and atomic commit atop BFTSMaRT, an open source BFT state machine replication library implementing a full-fledged adaptation of the PBFT consensus protocol. More information on BFTSMaRT can be found here: https://github.com/bft-smart/library

Please refer to the READMe on branch "main" for instructions on how to build and evaluate the systems.


