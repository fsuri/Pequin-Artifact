# Running experiments <a name="experiments"></a>
Hurray! You have completed the tedious process of installing the binaries and setting up Cloudlab. Next, we will cover how to run experiments in order to re-produce all results. This is a straightforward but time-consuming process, and importantly requires good network connectivity to upload binaries to the remote machines and download experiment results. Uploading binaries on high speed (e.g university) connections takes a few minutes and needs to be done only once per instantiated cloudlab experiment -- however, if your uplink speed is low it may take (as I have painstakingly experienced in preparing this documentation for you) several hours. Downloading experiment outputs requires a moderate amount of download bandwidth and is usually quite fast. This section is split into 4 subsections: 1) Pre-configurations for Hotstuff and BFTSmart, 2) Using the experiment scripts, 3) Parsing outputs, and finally 4) reproducing experiment claims 1-by-1.

Before you proceed, please confirm that the following credentials are accurate:
1. Cloudlab-username `<cloudlab-user>`: e.g. "fs435"
2. Cloudlab experiment name `<experiment-name>`: e.g. "pequin"
3. Cloudlab project name `<project-name`>: e.g. "pequin-pg0"  (May need the "-pg0" extension)

Confirm these by attempting to ssh into a machine you started (on the Utah cluster): `ssh <cloudlab-user>@us-east-1-0.<experiment-name>.<project-name>.utah.cloudlab.us`

### 1) Pre-configurations for Hotstuff and BFTSmart

When evaluating TxHotstuff and TxBFTSmart you will need to complete the following pre-configuration steps before running an experiment script:

1. **TxHotstuff**
   1. Navigate to `Pequin-Artifact/src/scripts`
   2. Run `./batch_size <batch_size>` to configure the internal batch size used by the Hotstuff Consensus module. See sub-section "1-by-1 experiment guide" for what settings to use
   3. Open file `config_remote.sh` and edit the following lines to match your Cloudlab credentials:
      - Line 3: `TARGET_DIR="/users/<cloudlab-user>/config/"`
      - Line 14: `rsync -rtuv config <cloudlab-user>@${machine}.<experiment-name>.<project-name>.utah.cloudlab.us:/users/<cloudlab-user>/`
   4. Finally, run `./config_remote.sh` 
   5. This will upload the necessary configurations for the Hotstuff Consensus module to the Cloudlab machines.

3. **TxBFTSmart**
   1. Navigate to `Pequin-Artifact/src/scripts`
   2. Run `./one_step_config.sh <Local Pequin-Artifact directory> <cloudlab-user> <experiment-name> <project-name> <cluster-domain-name>`
   3. For example: `./one_step_config.sh /home/floriansuri/Research/Projects/Pequin/Pequin-Artifact fs435 pequin pequin-pg0 utah.cloudlab.us`
   4. This will upload the necessary configurations for the BFTSmart Conesnsus module to the Cloudlab machines.
      - Troubleshooting: Make sure files `server-hosts` and `client-hosts` in `/src/scripts/` do not contain empty lines at the end

### 2) Using the experiment scripts

To run an experiment, you simply need to run: `python3 Pequin-Artifact/experiment-scripts/run_multiple_experiments.py <CONFIG>` using a specified configuration JSON file (see below). The script will load all binaries and configurations onto the remote Cloudlab machines, and collect experiment data upon completion. We have provided experiment configurations for all experiments claimed by the paper, which you can find under `Pequin-Artifact/experiment-configs`. In order for you to use them, you will need to make the following modifications to each file (Ctrl F and Replace in all the configs to save time):

#### Required Modifications:
1. `"project_name": "pequin-pg0"`
   - change the value field to the name of your Cloudlab project `<project-name>`. On cloudlab.us (utah cluster) you will generally need to add "-pg0" to your project_name in order to ssh into the machines. To confirm which is the case for you, try to ssh into a machine directly using `ssh <cloudlab-user>@us-east-1-0.<experiment-name>.<project-name>.utah.cloudlab.us`.  
2. `"experiment_name": "pequin"`
   - change the value field to the name of your Cloudlab experiment `<experiment-name>`.
3. `"base_local_exp_directory": “home/floriansuri/Research/Projects/Pequin/output”`
   - Set the value field to be the local path (on your machine or the control machine) where experiment output files will be downloaded to and aggregated. 
4. `"base_remote_bin_directory_nfs": “users/<cloudlab-user>/indicus”` 
   - Set the field `<cloudlab-user>`. This is the directory on the Cloudlab machines where the binaries will be uploaded
5. `"src_directory" : “/home/floriansuri/Research/Projects/Pequin/Pequin-Artifact/src”` 
   - Set the value field to your local path (on your machine or the control machine) to the source directory 
6. `"emulab_user": "<cloudlab-username>"`
   - Set the field `<cloudlab-user>`. 

#### **Optional** Modifications 
1. Experiment duration:
   - The provided configs are by default -- for time convenience -- set to run for 30 seconds total, using a warmup and cooldown period of 5 seconds respectively. 
      - "client_experiment_length": 30,
      - "client_ramp_down": 5,
      - "client_ramp_up": 5,
   - All experiment results in the paper were run for longer: 90 seconds total, with a warmup and cooldown period of 30 seconds respectively. If you want to run the experiments as long, replace the above settings with respective durations. For cross-validation purposes shorter experiments will suffice and save you time (and memory, since output files will be smaller).
   
2. Number of experiments:
   - The provided config files by default run the configured experiment once. Experiment results from the paper for 1-Workloads and 2-Failures were instead run several times (four times) and report the mean throughput/latency as well as standard deviations across the runs. For cross-validation purposes, this is not necessary. If you do however want to run the experiment multiple times, you can modify the config entry `num_experiment_runs: 1` to a repitition of your choice, which will automatically run the experiment the specified amount of times, and aggregate the joint statistics.
3. Number of clients:
   - The provided config files by default run an experiment for a single client setting that corresponds to the rough "peak" for throughput. Client settings are defined by the following JSON entries:
      - "client_total": [[71]],
         - "client_total" specifies the upper limit for total client *processes* used
      - "client_processes_per_client_node": [[8]],
         - "client_proccesses_per_client_node" specifies the number of client processes run on each server machine. 
      - "client_threads_per_process": [[2]],
         - "client_threads_per_process" specifies the number of client threads run by each client process.  
   - The *absolute total number* of clients used by an experiment is: **Total clients** *= max(client_total, num_servers x client_processes_per_client_node) *x client_threads_per_process*. For Tapir "num_servers" = 9, for Basil "num_servers" = 18, and for TxHotstuff/TxBFTSmart "num_servers" = 12.
   - To determine the peak **Total clients** settings we ran a *series* of client settings for each experiment. For simple cross-validation purposes this is not necessary - If you do however want to, you can run multiple settings automatically by specifying a list of client settings. For example:
      - "client_total": [[71, 54, 63, 71, 54, 63]],
      - "client_processes_per_client_node": [[8, 6, 7, 8, 6, 7]],
      - "client_threads_per_process": [[1, 2, 2, 2, 3, 3]]
   - For convenience, we have included such series (in comments) in all configuration files. To use them, uncomment them (by removing the underscore `_`) and comment out the pre-specified single settings (by adding an underscore `_`).
   - 
#### Starting an experiment:
You are ready to start an experiment. Use any of the provided JSON configs under `Pequin-Artifact/experiment-configs/<PATH>/<config>.json`. **Note that** all microbenchmark configs are Basil (Indicus) exclusive.

Run: `python3 <PATH>/Pequin-Artifact/experiment-scripts/run_multiple_experiments.py <PATH>Pequin-Artifact/experiment-configs/<PATH>/<config>.json` and wait!

Optional: To monitor experiment progress you can ssh into a server machine (us-east-1-0) and run htop. During the experiment run-time the cpus will be loaded (to different degrees depending on contention and client count).
  
   
### 3) Parsing outputs
After the experiment is complete, the scripts will generate an output folder at your specified `base_local_exp_directory`. Each folder is timestamped. 

To parse experiment results you have 2 options:
1. (Recommended) Looking at the `stats.json` file:
   1. Navigate into the timestamped folder, and keep following the timestamped folders until you enter folder `/out`. Open the file `stats.json`. When running multiple client settings, each setting will generate its own internal timestamped folder, with its own `stats.json` file. Multiple runs of the same experiment setting instead will directly be aggregated in a single `stats.json` file.
   2. In the `stats.json` file search for the Json field: `run_stats: ` 
   3. Then, search for the JSON field: `combined:`
   4. Finally, find Throughput measurments under `tput`, Latency measurements under `mean`, and Throughput per Correct client under `tput_s_honest` (**this will exist only for failure experiments**).
2. Looking at generated png plots:
   Alternatively, on your local machine you can navigate to `<time_stamped_folder>/plots/tput-clients.png` and `<time_stamped_folder>/plots/lat-tput.png` to look at the data points directly. Currently however, it shows as "Number of Clients" the number of total client **processes** (i.e. `client_total`) and not the number of **Total clients** specified above. Keep this in mind when viewing output that was generated for experiments with a list of client settings.
   
 Find below, some example screenshots from looking at a provided experiment output from `Pequin-Artifact/sample-output/Validated Results`:

   Experiment output folder:

   ![image](https://user-images.githubusercontent.com/42611410/129566751-a179de6e-8b22-49bc-96f5-bfb517e8eb9e.png)

   Subfolder that contains `stats.json`. Note: To save memory, we have removed all the server/client folders in /sample-output that you will see yourself.

   ![image](https://user-images.githubusercontent.com/42611410/129566648-808ea2d7-a2c0-48b4-b2e8-57221b040f13.png) 

   JSON fields `run_stats` and `combined`. Note: `combined` might not be the first entry within `run_stats` in every config, so double check to get the right data.

   ![image](https://user-images.githubusercontent.com/42611410/129566877-87000119-c43b-4fa2-973a-2a9e571d9351.png)

   Throughput: 

   ![image](https://user-images.githubusercontent.com/42611410/129566950-f0126263-7bd4-4978-8270-9051ad403a37.png)

   Latency: 

   ![image](https://user-images.githubusercontent.com/42611410/129566988-5fc99464-a6c2-4e7a-8108-320c55e5b82e.png)

   Correct Client Throughput: 

   ![image](https://user-images.githubusercontent.com/42611410/129567041-4f002dca-5c6f-4617-bab5-87d7f4bd1af0.png)

   Alternatively Plots (Throughput):

   ![image](https://user-images.githubusercontent.com/42611410/129566828-694cf8e2-2c25-4e5b-941e-9a745340ea74.png)


Next, we will go over each included experiment individually to provide some pointers.

### 4) Reproducing experiment claims 1-by-1
   
We have included recently re-validated experiment outputs (for most of the experiments) for easy cross-validation of the claimed througput (and latency) numbers under `/sample-output/ValidatedResults`. To directly compare against the numbers reported in our paper please refer to the figures there -- we include rough numbers below as well. Some of Basil' reported microbenchmark performances have changed slightly (documented below) as the codebase has since matured, but all takeaways remain consistent.

> :warning: Make sure to have set up a CloudLab experiment (with correct disk images matching your local/controllers package dependencies) and built all binaries locally before running (see instructions above).

#### **1 - Workloads**:
We report evaluation results for 3 workloads (TPCC, Smallbank, Retwis) and 4 systems: Tapir (Crash Failure baseline), Basil (our system), TxHotstuff (BFT baseline), and TxBFTSmart (BFT baseline). All systems were evaluated using 3 shards each, but use different replication factors.

   1. **Tapir**: 
 
   Reproducing our claimed results is straightforward and requires no additional setup besides running the included configs under `/experiment-configs/1-Workloads/1.Tapir`.  Reported peak results were roughly:
   
      - TPCC: Throughput: ~20k tx/s, Latency: ~7 ms
      - Smallbank: Throughput: ~ 61,5k tx/s, Latency: ~2.3 ms
      - Retwis: Throughput: ~45k tx/s, Latency: 2 ms
      All Tapir experiments were run using 24 shards to allow for even use of resources across systems, since unlike the BFT systems (all use 3 shards) that require multiple cores to handle cryptography, Tapir's servers are single threaded. \


   2. **Basil**: 
   
   Use the configurations under `/experiment-configs/1-Workloads/2.Basil`. Reported peak results were roughly:
   
      - TPCC: Throughput: ~4.8k tx/s, Latency: ~30 ms
      - Smallbank: Throughput: ~23k tx/s Latency: ~12 ms
      - Retwis: Throughput: ~24 k tx/s, Latency: ~10 ms
        
   > **[NOTE]** On both Smallbank and Retwis throughput has decreased (and Latency has increased) ever so slightly since the reported paper results, as the system now additionally implements failure handling, which is optimistically triggered even under absence of failures. To disable this option set the JSON value `"no_fallback" : "true"`. We note, that none of the baseline systems (implement and) run with failure handling.\
         
   3. **TxHotstuff:** 
   
   Use the configurations under `/experiment-configs/1-Workloads/3.TxHotstuff`. Before running these configs, you must configure Hotstuff using the instructions from section "1) Pre-configurations for Hotstuff and BFTSmart" (see above). Use a batch size of 4 when running TPCC, and 16 for Smallbank and Retwis for optimal results. Note, that you must re-run `src/scripts/remote_remote.sh` **after** updating the batch size and **before** starting an experiment. 
   
     Reported peak results were roughly:
      - TPCC: Throughput: ~920 tx/s, Latency: ~73 ms
      - Smallbank: Throughput: ~6.4k tx/s Latency: ~42 ms
      - Retwis: Throughput: ~5.2k tx/s, Latency: ~48 ms
      
   > :warning: **[WARNING]**: Hotstuffs performance can be quite volatile with respect to total number of clients and the batch size specified. Since the Hotstuff protocol uses a pipelined consensus mechanism, it requires at least `batch_size x 4` active client requests per shard at any given time for progress. Using too few clients, and too large of a batch size will get Hotstuff stuck. In turn, using too many total clients will result in contention that is too high, causing exponential backoffs which leads to few active clients, hence slowing down the remaining active clients. These slow downs in turn lead to more contention and aborts, resulting in no throughput. The configs provided by us roughly capture the window of balance that allows for peak throughput. \  
      
   4. **TxBFTSmart**: 
   
   Use the configurations under `/experiment-configs/1-Workloads/4.TxBFTSmart`. Before running these configs, you must configure Hotstuff using the instructions from section "1) Pre-configurations for Hotstuff and BFTSmart" (see above). You can, but do not need to manually set the batch size for BFTSmart (see optional instruction below). Note, that you must re-run `src/scripts/one_step_config.sh` **after** updating the batch size and **before** starting an experiment. 
      
      Reported peak results were roughly:
      - TPCC: Throughput: ~1.3k tx/s, Latency: ~60 ms
      - Smallbank: Throughput: ~8.7k tx/s Latency: ~19 ms
      - Retwis: Throughput: ~6.3k tx/s, Latency: ~23 ms

   > **[OPTIONAL NOTE]** **If you read, read fully**: To change batch size in BFTSmart navigate to  `src/store/bftsmartstore/library/java-config/system.config` and change line `system.totalordermulticast.maxbatchsize = <batch_size>`. Use 16 for TPCC and 64 for Smallbank/Retwis for optimal results. However, explicitly setting this batch size is not necessary, as long as the currently configured `<batch_size>` is `>=` the desired one. This is because BFTSmart performs optimally with a batch timeout of 0, and hence the batch size set *only* dictates an upper bound for consensus batches. Using a larger batch size has no effect. Hence, our reported optimal batch sizes of 16 and 64 respectively correspond to the upper bound after which no further improvements are seen. By default our configurations are set to `<batch_size> = 64`, so no further edits are necessary. \
   > **[Troubleshooting]**: If you run into any issues (specifically the error: “SSLHandShakeException: No Appropriate Protocol” ) with running BFT-Smart please comment out the following in your `java-11-openjdk-amd64/conf/security/java.security` file: `jdk.tls.disabledAlgorithms=SSLv3, TLSv1, RC4, DES, MD5withRSA, DH keySize < 1024 EC keySize < 224, 3DES_EDE_CBC, anon, NULL`


#### **2-Client Failures**:
   We evaluated 4 types of client failures: 1) forced simulated equivocation (equiv-forced), 2) realistic equivocation (equiv-real), 3) early client stalling (stall-early), and 4) late client stalling (stall-late). We evaluated all failures across 2 simple YCSB workloads: a) a uniform workload (RW-U), and b) a heavily skewed/contended workload (RW-Z). We remark once again that "equiv-forced" is **not** the realistic worst-case, as it simulates an artificial execution; Instead, it is "stall-early" that corresponds to the worst-case statistics claimed by us.
   
   The metric used is Throughput/Correct-Clients: It can be found under `stats.json` -> `run_stats` -> `combined` -> `tput_s_honest` (see subsection 3) Parsing outputs) /
   
   To reproduce the full lines/slope in the paper you need to re-run several configs per failure type, per workload. To only validate our claims, it may instead suffice to re-run data points near the maximum evaluated fault threshold (e.g. config Indicus-90-2). Configs are named after the total number of faulty clients, and the frequency at which they issue failures: E.g. Indicus-90-2.json (in a given failure type folder) simulates 90% of clients injecting a failure every 2nd transaction. To read the total percentage of faulty transaction (relative to total transactions injected) search for `"tx_attempted_failure_percentage"`.
   
   > ⚠️ **[WARNING]** Do NOT change the client settings in any of the configurations. To facilitate comparisons between different failure levels all must use the same number of clients. The client numbers used correspond to those reported in the paper.
   > **[NOTE]** Experiment stats are only collected on correct clients. Due to the high number of failure fractions evaluated, only few clients are used to collect stats which can lead to higher variance in the results; Hence all our results reported in the paper are averages over 4 runs and include standard deviation. You *may* want to do the same, but it is not necessary. We recommend using Indicus-90-2.json instead of 98-2.json for higher stability (if you only run one config).


   1. **Evaluating RW-U**: Use configs from `/experiment-configs/2-Client-Failures/RW-U
      1. No Failures baseline:
         - First, run the baseline `Indicus-NoFailures.json` to establish the throughput per correct client when no failures are occuring. 
         - Reported Tput/CorrectClients: ~107
      2. equiv-forced:
         - Navigate to folder `EquivForced` and run a config file.
         - Reported Tput for Indicus-90-2: Corresponds to 40% fautly/total tx. Tput/Corectclients: ~76
         
      3. equiv-real
         - Navigate to folder `EquivReal` and run a config file.
         - Reported Tput for Indicus-90-2: Corresponds to 45% faulty/total tx. Tput/Corectclients: ~107
         
      4. stall-early
         - Navigate to folder `StallEarly` and run a config file.
         - Reported Tput for Indicus-90-2: Corresponds to 46% faulty/total tx. Tput/CorrectClients: ~90
         
      5. stall-late
         - Navigate to folder `StallLate` and run a config file.
         - Reported Tput for Indicus-90-2: corresponds to 45% of faulty/total tx. Tput/CorrectClient: ~96
         
      All reported data points. Data format: [% faulty/total, Tput/CorrectClients]
      
            |  Failure Type | Indicus-10-3 | Indicus-15-2 | Indicus-30-3 | Indicus-60-3 | Indicus-60-2 | Indicus-80-2 | Indicus-90-2 | Indicus-98-2
            |---------------|--------------|--------------|--------------|--------------|--------------|--------------|--------------|-------------
            | equiv-forced  |  [3%, 104]   |  [7%, 100]   |  [10%, 97]   |  [19%, 39]   |  [28%, 83]   |  [36%, 79]   |  [40%, 76]   |  [43%, 74]
            | equiv-real    |      -       |       -      |  [10%, 107]  |  [20%, 106]  |  [30%, 106]  |  [40%, 107]  |  [45%, 107]  |  [49%, 107]
            | stall-early   |      -       |       -      |  [11%, 101]  |  [21%, 101]  |  [32%, 97]   |  [41%, 93]   |  [46%, 90]   |  [49%, 90]
            | stall-late    |      -       |       -      |  [10%, 106]  |  [20%, 104]  |  [30%, 102]  |  [40%, 100]  |  [45%, 90]   |  [49%, 95]
      
   2. **Evaluating RW-Z**: Use configs from `/experiment-configs/2-Client-Failures/RW-U
      1. No Failures baseline:
         - First, run the baseline `Indicus-NoFailures.json` to establish the throughput per correct client when no failures are occuring. 
         - Reported Tput/CorrectClients: ~67
      2. equiv-forced:
         - Navigate to folder `EquivForced` and run a config file.
         - Reported Tput for Indicus-90-2: Corresponds to 27% fautly/total tx. Tput/Corectclients: ~24
         
      3. equiv-real
         - Navigate to folder `EquivReal` and run a config file.
         - Reported Tput for Indicus-90-2: Corresponds to 36% faulty/total tx. Tput/Corectclients: ~66
         
      4. stall-early
         - Navigate to folder `StallEarly` and run a config file.
         - Reported Tput for Indicus-90-2: Corresponds to 40% faulty/total tx. Tput/CorrectClients: ~46
         
      5. stall-late
         - Navigate to folder `StallLate` and run a config file.
         - Reported Tput for Indicus-90-2: corresponds to 40% of faulty/total tx. Tput/CorrectClient: ~56
         
      All reported data points. Data format: [faulty transactions / total transactions as %, Tput/CorrectClients as tx/s/#correct_clients]
      
            |  Failure Type | Indicus-10-3 | Indicus-15-2 | Indicus-30-3 | Indicus-60-3 | Indicus-60-2 | Indicus-80-2 | Indicus-90-2 | Indicus-98-2
            |---------------|--------------|--------------|--------------|--------------|--------------|--------------|--------------|-------------
            | equiv-forced  |   [2%, 53]   |   [5%, 48]   |   [7%, 43]   |  [13%, 35]   |  [19%, 30]   |  [24%, 26]   |  [27%, 24]   |  [29%, 22]
            | equiv-real    |      -       |       -      |   [8%, 67]   |  [16%, 67]   |  [24%, 67]   |  [32%, 66]   |  [36%, 66]   |  [39%, 67]
            | stall-early   |      -       |       -      |   [10%, 65]  |  [18%, 60]   |  [29%, 55]   |  [36%, 50]   |  [40%, 46]   |  [42%, 44]
            | stall-late    |      -       |       -      |   [10%, 65]  |  [18%, 62]   |  [25%, 61]   |  [36%, 59]   |  [40%, 56]   |  [42%, 54]
     
 > **[NOTE]** Why does %faulty/total end at different points?/
     The explanation is not unavailability, or a failure to run the experiment: rather, it is an artefact of how we count transactions. In particular, (faulty_clients x failure_frequency) % (e.g. 45% for Indicus-90-2) of the transactions newly submitted to Basil are faulty. However, contention (and dependencies on equivocating transactions) can require some of the correct transactions to abort and re-execute (faulty transactions instead do not care to retry), which decreases the percentage of faulty transactions that Basil processes (since, again, some correct transactions end up being prepared multiple times). Thus, when measuring the throughout, the percentage of faulty transactions we report is the fraction of faulty transactions among all processed (as opposed to admitted) transactions--the latter is set at (faulty_clients x failure_frequency) %, while the former depends on the number of re-executions of correct transactions.
 

#### **Microbenchmarks**:
Finally, we review the reported Microbenchmarks.

#### **3-Crypto Overheads**:
To reproduce the reported evaluation of the impact of proofs and cryptography on the system navigate to `experiment-configs/3-Micro:Crypto`. The evaluation covers the RW-U workload as well as RW-Z, and for each includes a config with Crypto/Proofs disabled and enabled respectively. Since signatures induce a high amount of overhead the full Basil system is multithreaded and uses several worker threads to handle cryptography - Since this overhead falls to the wayside, the Non-Crypto/Proofs version instead runs single-threaded (no crypto worker threads) and instead uses the available cores to run more shards. In total, the Non-Crypto/Proofs version uses 24 shards, vs normal Basil using 3.

> **[NOTE]** Since running this microbenchmark the codebase has changed significantly to include client failure handling. The No-Crypto/Proofs option is no longer supported on the full version, and hence must run with the fallback protocol disabled (since failures are not simulated it will not regularly be triggered anyways, but it may be occasionally, leading to segmentation faults). The provided config has the fallback protocol disabled by default `"no_fallback": true"` - Make sure to not change this. We remark that the No-crypto/Proofs version is **not** a safe BFT implementation, it is purely an option for microbenchmarking overheads. The throughput for the No-Crypto/Proofs version has changed ever so slightly given the updates.

1. **RW-U**
   - Navigate to the `RW-U` folder and run `Indicus.json` and `Indicus-NoCrypto.json` respectively.
   - The reported results are ~38k tput for Crypto enabled (Indicus.json) and ~143k for Crypto/Proofs disabled.
2. **RW-Z**
   - Navigate to the `RW-Z` folder and run `Indicus.json` and `Indicus-NoCrypto.json` respectively.
   - The reported results are ~4.8k tput for Crypto enabled (Indicus.json) and ~22k for Crypto/Proofs disabled.

#### **4-Reads**:
To reproduce the reported evaluation of the impact of different Read Quorum sizes on the system navigate to `experiment-configs/4-Micro:Reads`. The evaluation uses a read only workload and compares Read Quorums consisting of 1) a single read, 2) f+1 reads from different replicas, and 3) 2f+1 reads from different replicas. All configurations use an "eager-reads" optimization (which is used by all baseline systems too) in which read messages are optimistically only sent to the Read Quorum itself (instead of pessimistically sending to f additional replicas).

> ⚠️**[Warning]** Do **not** run the single read configuration on a non-read-only workload (i.e. a workload with writes as well) as the prototype is hard coded to only read uncommitted values from f+1 replicas (which is necessary for Byzantine Independence). Running with a single read is **not** BFT tolerant and is purely an option for microbenchmarking purposes.

The provided configs only run an experiment for the rough peak points reported in the paper which is sufficient to compare the overheads of larger Quorums. If you want to reproduce the full figure reported, you may run `combined.json`, however we advise against it, since it takes a *considerable* amount of time. You may instead run each configuration for a few neighboring client configurations (already included as comments in the configs). 

1. **Single read**
   - Run configuration `1.json`.
   - The reported peak throughput is ~17k tx/s.
2. **f+1 reads**
   - Run configuration `f+1.json`.
   - The reported peak throughput is ~13.5k tx/s
3. **2f+1 reads**
   - Run configuration `2f+1.json`.
   - The reported peak throughput is ~17k tx/s
  
#### **5-Sharding**:
To reproduce the reported evaluation of the impact of proofs and cryptography on sharding navigate to `experiment-configs/5-Micro:Sharding`. The evaluation covers the RW-U workload and includes configurations for different number of shards with Crypto/Proofs disabled and enabled respectively. Since the Non-Crypto/Proofs version is single threaded (see subsection 3-Crypto above) we run 8 times more shards than in the full Basil system, since the latter uses 8 threads, over 8 cores (since m510 machines have 8 cores).

> **[NOTE]** Like mentioned under **3-Crypto** above, th No-Crypto/Proofs option is no longer supported on the full Basil prototype  and hence must run with the fallback protocol disabled The provided config has the fallback protocol disabled by default `"no_fallback": true"` - Make sure to not change this. 

1. **Crypto/Proofs enabled** (normal Basil): 
   - Navigate to folder `/Crypto`.
   1. Scale Factor 1 (1 shard): 
      - Run config `1-Indicus-RW-U.json`
      - Reported throughput: ~20k
   2. Scale Factor 2 (2 shards): 
      - Run config `2-Indicus-RW-U.json`
      - Reported throughput: ~23k
   3. Scale Factor 3 (3 shards): 
      - Run config `3-Indicus-RW-U.json`
      - Reported throughput: ~27k

1. **Crypto/Proofs disabled**: 
   - Navigate to folder `/Non-Crypto`.
   1. Scale Factor 1 (8 shards): 
      - Run config `8-RW-U-cryptoOFF.json`
      - Reported throughput: ~45k
   2. Scale Factor 2 (16 shards): 
      - Run config `16-RW-U-cryptoOFF.json`
      - Reported throughput: ~61k
   3. Scale Factor 3 (24 shards): 
      - Run config `24-RW-U-cryptoOFF.json`
      - Reported throughput: ~86k

   > Throughput may be a little better on the current version - which only emphasizes the overhead that Crypto and Quroum Proofs impose.


#### **6-FastPath**:
To reproduce the reported evaluation of the utility of the Fast Path navigate to `experiment-configs/6-Micro:FastPath`. The evaluation covers both the RW-U and RW-Z workload and includes configurations to run the normal Basil prototype, and the Basil prototype with the Fast Path explicitly disabled.

1. **RW-U**
   - Navigate to the `RW-U` folder and run `Indicus_16_FP_ON.json` and `Indicus_16_FP_OFF.json` to run Basil with Fast Path enabled and disabled respectively.
   - The reported results are ~38k tput for Fast Path enabled, and ~32k for Fast Path disabled.
2. **RW-Z**
   - Navigate to the `RW-Z` folder and run `Indicus_4_FP_ON.json` and `Indicus_4_FP_OFF.json` to run Basil with Fast Path enabled and disabled respectively.
   - The reported results are ~4.8k tput for Fast Path enabled and ~2.4k for Fast Path disabled.
   > The RW-Z results for Fast-Path disabled may be slightly higher (which is a good thing) than the reported results since we modified the codebase since.

   > [Note] The evaluation with no Fast Path is a lower bound on the impact of a replica failure during the Prepare phase. While Basil cannot use the Commit Fast Path in presence of a misbhehaving replica, it might still be able to use the Abort Fast Path, which reduces contention by removing tentative transactions faster, and allows clients submitting aborting transactions to retry sooner. This microbenchmark instead fully disables all Fast Paths.
     
#### **7-Batching**:
To reproduce the reported evaluation of different batch sizes in Basil navigate to `experiment-configs/7-Micro:Batching`. The evaluation covers both the RW-U and RW-Z workload and includes configurations to run Basil with several different batch sizes. 

1. **RW-U**
   - Navigate to the `RW-U` folder and run different batch sizes by running `Indicus_<batch_size>.json`.
   - The reported peak results are for batch size 16/31 at ~38k at which point there are no further improvements
2. **RW-Z**
   - Navigate to the `RW-Z` folder and run different batch sizes by running `Indicus_<batch_size>.json`.
   - The reported peak results are for batch size 2/4 at ~4.8k at which point further increasing the batch size is detrimental.
   - The RW-Z results for batch sizes 1 and 8 may be slightly higher than the reported results since we modified the codebase since, but the peak remains the same.
       
All reported data points. 
     
            | Workload | Batch Size: 1 | Batch Size: 2 | Batch Size: 4 | Batch Size: 8 | Batch size: 16 | Batch size: 32 |
            |----------|---------------|---------------|---------------|---------------|----------------|----------------|
            |   RW-U   |   9600 tx/s   |   14400 tx/s  |   21700 tx/s  |   30300 tx/s  |   38200 tx/s   |  38200 tx/s    |
            |   RW-Z   |   3300 tx/s   |    4600 tx/s  |    4800 tx/s  |    2900 tx/s  |        -       |        -       |
