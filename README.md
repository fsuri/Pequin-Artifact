# BFT Query Processing -- Pequin Artifact 
This is the repository for the code artifact of "Pequin: Spicing up BFT with Query Processing".

For all questions about the artifact please e-mail Florian Suri-Payer <fsp@cs.cornell.edu>. For specific questions about the following topics please additionally CC:
1) Peloton: <giridhn@berkeley.edu>
2) Postgres: <sc3348@cornell.edu>
3) CockroachDB: <larzola@ucsd.edu>



# Table of Contents
1. [High Level Claims](#Claims)
2. [Artifact Organization](#artifact)
3. [Overview of steps to validate Claims](#validating)
4. [Installing Dependencies and Building Binaries](Installation.md)
5. [Setting up CloudLab](CloudlabSetup.md)
6. [Running Experiments](RunningExperiments.md)

## Claims 

### General

This artifact contains, and allows to reproduce, experiments for all figures included in the paper "Pesto: Cooking up high performance BFT Queries". 

It contains a prototype implemententation of Pesto, a replicated Byzantine Fault Tolerant Database offering a interactive transaction using a SQL interface. The prototype uses cryptographically secure hash functions and signatures for all replicas, but does not sign client requests on any of the evaluated prototype systems, as we delegate this problem to the application layer. The Pesto prototype can simulate Byzantine Clients failing via Stalling or Equivocation, and is robust to both. While the Basil prototype tolerates many obvious faults such as message corruptions and duplications, it does *not* exhaustively implement defences against arbitrary failures or data format corruptions, nor does it simulate all possible behaviors. For example, while the prototype implements fault tolerance (safety) to leader failures during recovery, it does not include code to simulate these, nor does it implement explicit exponential timeouts to enter new views that are necessary for theoretical liveness under partial synchrony.

# Prototypes: Pesto, Peloton, Peloton-HS and Peloton-Smart

This repository includes prototypes for Pesto, Peloton, Peloton-HS, Peloton-Smart, Postgres, CockroachDB, and several others not used for the evaluation of Pesto: Basil, Tapir, TxHotstuff, TxBFTSmart.

**Pesto** is a Byzantine Fault Tolerant SQL Database. Transaction processing in Pesto is client-driven, and independent of other concurrent but non-conflicting transactions. Pesto allows queries to execute, in the common case, in a single round-trip, and in two otherwise (subject to contention). Transactions can commit across shards in just a single round trip in absence of failures, and at most two under failure.
This combination of low latency and parallelism allows Pesto to scale beyond transactional database systems built atop strongly consistent BFT SMR protocols. 
The Pesto prototype ('pequinstore') uses as starting point the implementation of Basil ('indicusstore'). The codebase is (to the best of our knowledge) backwards compatible with the Basil SOSP'21 artifact -- for documentation about Basil (and its baselines) please refer to the [original document](https://github.com/fsuri/Basil_SOSP21_artifact).

We use Protobuf and TCP for networking, [ed25519](https://github.com/floodyberry/ed25519-donna) elliptic-curve digital signatures and [HMAC-SHA256](https://github.com/weidai11/cryptopp) for authentication, and [Blake3](https://github.com/BLAKE3-team/BLAKE3) for hashing. For its data store, \sys{} adapts [Peloton](https://github.com/cmu-db/peloton), a full fledged open-source SQL Database based on [Postgres](https://www.postgresql.org/). We modify Peloton to use \sys{}'s Concurrency Control (CC) and support snapshot sourcing and materialization. Specifically, we 1) remove Peloton's native CC, 2) modify execution to keep track of Read sets, Snapshots, TableVersions, and predicates, 3) change row formats to include Pesto-specific meta data, and 4) implement support for writing and rolling back prepared transactions.
We further remove Peloton's self-driving configuration features and llvm options, and make several optimizations to its index selection and execution procedures.

> **[NOTE]** The Pesto prototype codebase is henceforth called "*Pequin*". Throughout this document you will find references to Pequin, and many configuration files are named accordingly. All these occurences refer to the Pesto prototype.

**Peloton**. [Peloton](https://github.com/cmu-db/peloton) is a fully fledged SQL database based on [Postgres](https://www.postgresql.org/). Our unreplicated Peloton prototype adopts our non Pesto-specific optimizations, and is run in-memory on a dedicated server proxy. Clients connect to the proxy, which sequences operations within the same transaction, and executes operations from different transactions in parallel.

**Peloton-SMR** is a strawman system that modularly layers Peloton atop BFT state machine replication (SMR). We instantiate Peloton-SMR in two flavors, **Peloton-Smart** and **Peloton-HS** which, respectively, layer Peloton atop BFT-SMaRt~\cite{bessani2014state, bftsmart}, and HotStuff~\cite{yin2019hotstuff}. For correctness, SMR-based designs require deterministic execution on each replica: this demands either sequential execution -- which drastically limits performance --, or implementation of sophisticated custom parallel execution engines~\cite{gelashvili2023block}. Pesto, in contrast, allows for optimal parallelism by design. To facilitate a fair comparison, we opt to relax the determinism requirement for Peloton-SMR: we allow replicas to freely execute transactions in parallel, and designate a "primary" replica to respond to clients to ensure serializability. This system configuration is explicitly NOT FAULT TOLERANT, but simulates the optimal upper-bound on achievable performance. 

> **[NOTE]** The Peloton-SMR prototype *can* be operated with sequential execution (true SMR) but performance will suffer significantly (i.e. the benefit of Pesto will become even larger). Pesto's design, in contrast, is naturally parallel which drastically simplifies execution; there is no need for implementation of complex and custom deterministic parallel execution engines.


Finally, we augment both Peloton-SMR prototypes to benefit from Basil's reply batching scheme~\cite{suri2021basil} to amortize signature generation overheads.

**BFTSMaRT** is an open source BFT state machine replication library implementing a full-fledged adaptation of the PBFT consensus protocol. More information on BFTSMaRT can be found here: https://github.com/bft-smart/library. We adopt the code-base as is, and implement an interface to Peloton using JNI.

**HotStuff**
We use the `libhotstuff` implemenation, an open source BFT state machine replication library written by the HotStuff authors. More information on Hotstuff can be found here: https://github.com/hot-stuff/libhotstuff. libhotstuff, by default, implements no batch timer and proposes only fixed-sized batches. This is inefficient and creates tension between batch sizes: a low batch size fails to optimally amortize consensus overheads, while a high batch size may cause progress to stall; this concern is amplified by HotStuff's pipelined nature (it takes 4 batches to commit one proposal).
Inspired by BFT-Smart, we optimize libhotstuffs batching procedure to allow proposals to use flexibly sized batches: if load is low, this allows HotStuff to issue proposals nonetheless (thus avoiding stalling); if load is high, this allows HotStuff to pack more requests into a batch (up to a cap), thus avoiding waiting.

[**Postgres**](https://www.postgresql.org/) is a production grade open-source SQL database. We run two variants of Postgres: 1) unreplicated Postgres, and 2) Postgres configured with its native primary-backup support (Postgres-PB).
We mount Postgres atop tempfs to avoid disk accesses. `src/scripts/postgres_service.sh` details our parameterization. We allocate ample memory and buffer space.


**CockroachDB** is a production grade distributed database. 

TODO: Describe config
Additional detail on our CRDB setup:
       - we start each CockroachDb node using `cockroach start` for a multi-node cluster or `cockroach start-single-node` in the case of a single node. 
       - we use the `--insecure` flag to disable TLS encryption. 
       - we set the listening address and port for incoming connections using `--listen-addr`. 
       - the `--join` flag is used to specify the list of other nodes in the cluster. 
       - `--http-addr` is used to specify the address for the database console UI. 
       - The storage configuration is set to run in memory using up to 100% memory capacity using `--store=type=mem,size=1.0`. 
       - We disable logging except for logs at the FATAL level in the "OPS", "HEALTH", and "SQL_SCHEMA" channels. 
       - The last node in the deployment sequence is used to initialize the cluster. 
       - We set a lock timeout of 50ms to enhance performance under contention using `ALTER DATABASE defaultdb SET lock_timeout = '50ms';`. 
       - We set the minimum bytes for a range to 0 to improve sharding over tables like `Warehouse` in TPCC, which has few rows with few columns but high contention. 
       - We set the maximum bytes for a range to 134217728 (128Mb) and the number of replicas to 1 to disable replication (for now). 
       - The last node is used to generate the HA Proxy configuration which is used by CRDB for load balancing. 
       - We load balance client traffic to each server by having each client process send traffic to the server node corresponding to `client_id % number_of_servers`.

## Benchmarks:
We implement four benchmarks:

**TPCC**  
[TPC-C](https://tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf) simulates the business of an online e-commerce application. It consists of 5 transaction types, allowing clients to issue new orders and payments, fulfill deliveries, and query current order status and item stocs.
TPC-C exhibits high contention as most transactions read and write to the Warehouse table. TPC-C is a point read heavy workload -- many queries can therefore be satisfied using Pesto's dedicated point read protocol. 
We configure it to use 20 warehouses, and instantiate indexes to retrieve orders by customer, as well as customers by their last name.

 -- re-implement in C++ based off java benchbase code. Make some misc changes/fixes 
**Auctionmark** simulate an online auction sales portal. It consists of 9 transaction types, allowing clients to create and bid for items, confirm purchases, and write comments and feedback. Items are skewed by users: the majority of items are owned by a select few "heavy" sellers; the majority of sellers only manage a few items.
The workload is characterized by a high fraction of range queries and cross-table joins, but exhibit overall low contention relative to TPC-C. 

**Seats** simulates an airline booking system. It consists of 6 transaction types, allowing clients to search for flights and open seats, create, update and delete reservations, as well as update customer information.
Access is distributed uniformly across customers and flights. The workload is characterized by a high fraction of range queries and cross-table joins, but, like Auctionmark, exhibits overall low contention relative to TPC-C. 


We model our Auctionmark and Seats implementation after [Benchbase](https://github.com/cmu-db/benchbase), making minor fixes for correctness.We instantiate both workloads with a "ScaleFactor" of 1.

**YCSB-Tables**
Finally, we implement a custom read-modify-write microbenchmark based on YCSB. The database can be instantiated with a configurable amount of tables and rows; each row contains a key-value pair. Keys are unique (primary key), while values can either be random or fall within a configurable number of candidate categories.
Transactions read and/or write to a configurable number of rows; reads may optionally be conditioned on a secondary condition (e.g. value category). The access pattern to both tables and rows within tables is configurable: it may be uniformly random, or follow a YCSB-based Zipfian (coefficient configurable).


### Concrete claims in the paper

- **Main claim 1**: Pesto's throughput is within a small factor (roughly equal on TPCC, within 1.36x on Auctionmark, and 1.22x on Seats) of that of Peloton, an unreplicated SQL database that forms the basis of Pesto's execution engine.

- **Main claim 2**: Pesto achieves higher throughput and lower latency than both Peloton-SMR baselines (2.3x throughput on TPCC, 1.1x on Auctionmark/Seats; latency 3.9x/2.7x under Peloton-HS/-Smart on TPCC, 5x/3x on Auctionmark, and 4.6x/3.4x on Seats)

   All comparisons for claims 1 and 2 are made in the absence of failures.

- **Supplementary**: All other microbenchmarks reported realistically represent Pesto.


## Artifact Organization <a name="artifact"></a>

The core prototype logic of each system is located in the following folders: 
1. `src/store/pequinstore`: Contains the source code implementing the Pesto protype (Pequin).
2. `src/store/pelotonstore`: Contains the source code implementing the Peloton protype. Can be configured via `SMR_mode` flag to run unreplicated, with HotStuff, or with BFT-Smart.

The respective HotStuff and BFT-Smart modules are found in
3. `src/store/hotstuffstore/libhotstuff`: Contains the source code for the Hotstuff SMR module.
4. `src/store/bftsmartstore/library`: Contains the source code for the BFTSmart SMR module.

Benchmarks are located under `src/store/benchmark/async/sql`.

Networking and cryptography functionality is found in `src/lib`.

The experiment scripts to run all prototypes on CloudLab are found in `experiment-scripts`. `src/` and `src/scripts` contain additional helper scripts used to create/upload benchmark data, and pre-configure HotStuff/BFT-Smart.
Finally, `experiment-configs` contains the configs we used in our experiments.


## Validating the Claims - Overview <a name="validating"></a>

All our experiments were run using Cloudlab (https://www.cloudlab.us/), specifically the Cloudlab Utah cluster. To reproduce our results and validate our claims, you will need to 1) instantiate a matching Cloudlab experiment, 2) build the prototype binaries, and 3) run the provided experiment scripts with the (supplied) configs we used to generate our results. You may go about 2) and 3) in two ways: You can either build and control the experiments from a local machine (easier to parse/record results & troubleshoot, but more initial installs necessary); or, you can build and control the experiments from a dedicated Cloudlab control machine, using pre-supplied disk images (faster setup out of the box, but more overhead to parse/record results and troubleshoot). Both options are outlined in this ReadMe.

The ReadMe is organized into the following high level sections. Refer to each link for detailed documentation:

1. [*Installing pre-requisites and building binaries*](Installation.md)

   To build Pesto and baseline source code several dependencies must be installed. Refer to section "Installing Dependencies" for detailed instructions on how to install dependencies and compile the code. You may skip this step if you choose to use a dedicated Cloudlab "control" machine using *our* supplied fully configured disk images. Note that, if you choose to use a control machine but not use our images, you will have to follow the Installation guide too, and additionally create your own disk images. More on disk images can be found in section "Setting up Cloudlab".
  

2. [*Setting up experiments on Cloudlab* ](CloudlabSetup.md)

     To re-run our experiments, you will need to instantiate a distributed and replicated server (and client) configuration using Cloudlab. We have provided a public profile as well as public disk images that capture the configurations we used to produce our results. Section "Setting up Cloudlab" covers the necessary steps in detail. Alternatively, you may create a profile of your own and generate disk images from scratch (more work) - refer to section "Setting up Cloudlab" as well for more information. Note, that you will need to use the same Cluster (Utah) and machine types (m510) to reproduce our results.


3. [*Running experiments*](RunningExperiments.md)

     To reproduce our results you will need to build the code, and run the supplied experiment scripts using the supplied experiment configurations. Section "Running Experiments" includes instructions for using the experiment scripts, modifying the configurations, and parsing the output. TxHotstuff and TxBFTSmart require additional configuration steps, also detailed in section "Running Experiments".
     
