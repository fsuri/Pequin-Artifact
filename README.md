# BFT Query Processing -- Pequin Artifact 
This is the repository for the code artifact of "Pequin: Spicing up BFT with Query Processing".

For all questions about the artifact please e-mail Florian Suri-Payer <fsp@cs.cornell.edu>. For specific questions about the following topics please additionally CC:
1) Peloton: <giridhn@berkeley.edu>
2) CockroachDB: <liam0215@gmail.com>



# Table of Contents
1. [High Level Claims](#Claims)
2. [Artifact Organization](#artifact)
3. [Overview of steps to validate Claims](#validating)
4. [Installing Dependencies and Building Binaries](#installing)
5. [Setting up CloudLab](#cloudlab)
6. [Running Experiments](#experiments)

## Claims 

### General

This artifact contains, and allows to reproduce, experiments for all figures included in the paper "Pesto: Cooking up high performance BFT Queries". 

It contains a prototype implemententation of Pesto, a replicated Byzantine Fault Tolerant Database offering a interactive transaction using a SQL interface. The prototype uses cryptographically secure hash functions and signatures for all replicas, but does not sign client requests on any of the evaluated prototype systems, as we delegate this problem to the application layer. The Pesto prototype can simulate Byzantine Clients failing via Stalling or Equivocation, and is robust to both. While the Basil prototype tolerates many obvious faults such as message corruptions and duplications, it does *not* exhaustively implement defences against arbitrary failures or data format corruptions, nor does it simulate all possible behaviors. For example, while the prototype implements fault tolerance (safety) to leader failures during recovery, it does not include code to simulate these, nor does it implement explicit exponential timeouts to enter new views that are necessary for theoretical liveness under partial synchrony.

# Prototypes: Pesto, Peloton, Peloton-HS and Peloton-Smart

This repository includes the prototype code used for "Basil: Breaking up BFT with ACID (transactions)" as well as "TAPIR -- the Transaction Application Protocol for Inconsistent Replication." 

The codebase builds on Basil and TAPIR. 
TAPIR is a protocol for linearizable distributed transactions built using replication with no consistency guarantees. By enforcing consistency only at the transaction layer, TAPIR eliminates coordination at the replication layer, enabling TAPIR to provide the same transaction model and consistency guarantees as existing systems, like Spanner, with better latency and throughput.
More information on TAPIR can be found here: https://github.com/UWSysLab/tapir.

Basil is a Byzantine Fault Tolerant system that implements distributed and interactive transactions, that like TAPIR does not rely on strong consistency at the replication level. Basil allows transactions to commit across shards in just a single round trip in the common case, and at most two under failure. Transaction processing in Basil is client-driven, and independent of other concurrent but non-conflicting transactions. This combination of low latency and parallelism allows Basil to scale beyond transactional systems built atop strongly consistent BFT SMR protocols. 

Additionally, this repository includes prototype code implementing two transactional key-value stores -- "TxHotstuff" and "TxBFTSmart" -- that serve as BFT baseline systems to compare against Basil. TxHotstuff implements distributed transactions and atomic commit atop libhotstuff, an open source BFT state machine replication library implementing the Hotstuff protocol. More information on Hotstuff can be found here: https://github.com/hot-stuff/libhotstuff.
TxBFTSmart implements distributed transactions and atomic commit atop BFTSMaRT, an open source BFT state machine replication library implementing a full-fledged adaptation of the PBFT consensus protocol. More information on BFTSMaRT can be found here: https://github.com/bft-smart/library.

> **[NOTE]** The Basil prototype codebase is henceforth called "*Indicus*". Throughout this document you will find references to Indicus, and many configuration files are named accordingly. All these occurences refer to the Basil prototype.

Basils current codebase (Indicus) was modified beyond some of the results reported in the paper to include the fallback protocol used to defend against client failures. While takeaways remain consistent, individual performance results may differ slightly across the microbenchmarks (better performance in some cases) as other minor modifications to the codebase were necessary to support the fallback protocol implementation.

In addition to Basil, this artifact contains prototype implementations for three baselines: 1) An extension of the original codebase for Tapir, a Crash Failure replicated and sharded key-value store, as well as 2) TxHotstuff and 3) TxBFTSmart, two Byzantine Fault Tolerant replicated and sharded key-value stores built atop 3rd party implementations of consensus modules. 

## Benchmarks:

TPCC

Auction

Seats

YCSB-Tables

### Concrete claims in the paper

- **Main claim 1**: Pesto's throughput is within a small factor (within TODOx on TPCC, TODOx on Auctionmark, and TODOx on Seats) of that of Peloton, an unreplicated SQL database that forms the basis of Pesto's execution engine.

- **Main claim 2**: Pesto achieves higher throughput and lower latency than both BFT baselines (>5x over TxHotstuff on TPCC, 4x on Smallbank, and close to 5x on Retwis; close to 4x over TxBFTSmart on TPCC, 3x on Smallbank, and 4x on Retwis).

   All comparisons for claims 1 and 2 are made in the absence of failures.

- **Supplementary**: All other microbenchmarks reported realistically represent Basil.


## Artifact Organization <a name="artifact"></a>

The core prototype logic of each system is located in the following folders: 
1. `src/store/pequinstore`: Contains the source code implementing the Pesto protype logic (Pequin).
2. `src/store/pelotonstore`: Contains the source code implementing the Tapir protype logic. Tapir makes use of the Inconsistent Replication module located under `/src/replication/ir`
3. `src/store/hotstuffstore`: Contains the source code implementing the TxHotstuff protype. Includes `/libhotstuff`, which contains the Hotstuff SMR module.
4. `src/store/bftsmartstore`: Contains the source code implementing the TxBFTSmart protype. Includes `/library`, which contains the BFTSmart SMR module.


## Validating the Claims - Overview <a name="validating"></a>

All our experiments were run using Cloudlab (https://www.cloudlab.us/), specifically the Cloudlab Utah cluster. To reproduce our results and validate our claims, you will need to 1) instantiate a matching Cloudlab experiment, 2) build the prototype binaries, and 3) run the provided experiment scripts with the (supplied) configs we used to generate our results. You may go about 2) and 3) in two ways: You can either build and control the experiments from a local machine (easier to parse/record results & troubleshoot, but more initial installs necessary); or, you can build and control the experiments from a dedicated Cloudlab control machine, using pre-supplied disk images (faster setup out of the box, but more overhead to parse/record results and troubleshoot). Both options are outlined in this ReadMe.

The ReadMe is organized into the following high level sections:

1. [*Installing pre-requisites and building binaries*](Installation.md)

   To build Pesto and baseline source code several dependencies must be installed. Refer to section "Installing Dependencies" for detailed instructions on how to install dependencies and compile the code. You may skip this step if you choose to use a dedicated Cloudlab "control" machine using *our* supplied fully configured disk images. Note that, if you choose to use a control machine but not use our images, you will have to follow the Installation guide too, and additionally create your own disk images. More on disk images can be found in section "Setting up Cloudlab".
  

2. [*Setting up experiments on Cloudlab* ](CloudlabSetup.md)

     To re-run our experiments, you will need to instantiate a distributed and replicated server (and client) configuration using Cloudlab. We have provided a public profile as well as public disk images that capture the configurations we used to produce our results. Section "Setting up Cloudlab" covers the necessary steps in detail. Alternatively, you may create a profile of your own and generate disk images from scratch (more work) - refer to section "Setting up Cloudlab" as well for more information. Note, that you will need to use the same Cluster (Utah) and machine types (m510) to reproduce our results.


3. [*Running experiments*](RunningExperiments.md)

     To reproduce our results you will need to build the code, and run the supplied experiment scripts using the supplied experiment configurations. Section "Running Experiments" includes instructions for using the experiment scripts, modifying the configurations, and parsing the output. TxHotstuff and TxBFTSmart require additional configuration steps, also detailed in section "Running Experiments".
     
