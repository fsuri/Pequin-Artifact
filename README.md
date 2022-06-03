
# TxHotstuff and TxBFTSmart

This repository includes prototype code implementing two transactional key-value stores, "TxHotstuff" and "TxBFTSmart", as BFT baseline system to compare against Basil. TxHotstuff implements distributed transactions and atomic commit atop libhotstuff, an open source BFT state machine replication library implementing the Hotstuff protocol. More information on Hotstuff can be found here: https://github.com/hot-stuff/libhotstuff
TxBFTSmart implements distributed transactions and atomic commit atop BFTSMaRT, an open source BFT state machine replication library implementing a full-fledged adaptation of the PBFT consensus protocol. More information on BFTSMaRT can be found here: https://github.com/bft-smart/library

Please refer to the READMe on branch "main" for instructions on how to build and evaluate the system.
