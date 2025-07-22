# Running experiments <a name="experiments"></a>
Hurray! You have completed the tedious process of installing the binaries and setting up Cloudlab. 
Next, we will cover how to run experiments in order to re-produce all results. This is a straightforward but time-consuming process.

Ideally you have good network connectivity to quickly upload binaries to the remote machines and download experiment results. 
Uploading binaries on high speed connections (e.g at your university) takes a few minutes and needs to be done only once per instantiated Cloudlab experiment -- however, if your uplink speed is low it may take (as I have painstakingly experienced in preparing this documentation for you) several hours. Downloading experiment outputs requires a moderate amount of download bandwidth and is usually quite fast.

This section is split into 5 subsections: 
1. [Preparing Benchmarks](#prep)
2. [Pre-configurations for HotStuff, BFTSmart, and Postgres](#preconfig)
3. [Experiment script instructions](#scripts)
4. [Parsing outputs](#output)
5. [Reproducing our experiments 1-by-1](#exp)


Before you proceed, please confirm that your CloudLab credentials are accurate:
1. Cloudlab-username `<cloudlab-user>`: e.g. "fs435"
2. Cloudlab experiment name `<experiment-name>`: e.g. "pequin"
3. Cloudlab project name `<project-name`>: e.g. "pequin-pg0"  (May need the "-pg0" extension)

Confirm these by attempting to ssh into a machine you started (on the Utah cluster): `ssh <cloudlab-user>@us-east-1-0.<experiment-name>.<project-name>.utah.cloudlab.us`

## High level experiment checklist
Running experiments involves 5 steps. Refer back to this checklist to stay on track!

> :warning: Make sure to have set up a CloudLab experiment (with correct disk images matching your local/controllers package dependencies) and built all binaries locally before running!

1. The first step is to generate and upload initial data used by the benchmarks
2. Next, if you're running an SMR-based store (e.g. Peloton-HS or Peloton-Smart), you will need to pre-configure the SMR module. The exact procedure depends on the module you are using. Postgres requires some pre-setup as well.
3. In order to run an experiment, you will need to write a configuration file (or copy and adjust our pre-supplied configs). This specifies the cluster setup, the benchmark to run, and the parameters of the system.
4. You're ready to run the experiment! Run the experiment script and supply it with your prepared config.
5. Finally, inspect the downloaded experiment run by checking the output data. 

## (1) Preparing Benchmarks <a name="prep"></a>

> :warning: Make sure that the names of your CloudLab machines match those in the helper scripts!

To generate benchmark data simple run the script `src/generate_benchmark_data.sh`. Configure it as follows:
1) specify the benchmark you want to generate, e.g. to run TPC-C use `-b 'tpcc'`
2) specify the benchmark parameters, e.g. to create 20 warehouses for TPC-C use `-n 20`

> 📓 You may need to enable permissions for the scripts before running: `cmod +x <scriptname>`

- Generate TPC-C data using: `./generate_benchmark_data -n 20` (tpcc is the default benchmark)
- Generate Auctionmark data using: `./generate_benchmark_data -b 'auctionmark'` (using default scale factor)
- Generate Seats data using: `./generate_benchmark_data -b 'seats'` (using default scale factor)

Once you created the benchmark data (you can create all data upfront), upload the respective benchmark data to your CloudLab cluster using `src/upload_data_remote`.
Simply specify which benchmark you are uploading, and to how many shards (1, 2 or 3) you are uploading. You can also pass in your cloudlab user and expeirment name.
- E.g. use `./upload_data_remote -b 'tpcc' -s 2 -u `fs435`` to upload TPC-C data to 2 shards, with cloudlab user `fs435`
- TPC-C and 1 shard are default parameters. Check our the script for exact usage!

Note: Benchmark data, by default, is uploaded to `/users/<cloudlab-user>/benchmark_data/`. 

## (2) Pre-configurations for Hotstuff, BFTSmart, and Postgres <a name="preconfig"></a>

When evaluating Peloton-HS, Peloton-Smart, or Postgres you will need to complete the following pre-configuration steps before running an experiment script:

### **Hotstuff**
   1. Navigate to `Pequin-Artifact/src/scripts`
   2. [**OPTIONAL**] Run `./batch_size <batch_size>` to configure the internal batch size used by the HotStuff consensus module. See sub-section "1-by-1 experiment guide" for what settings to use. The default value is an *upper* cap of 200. Since we modified Hotstuff to use more efficient, dynamic batch sizes, changing the default batch cap is not necessary.
   3. Run `./pghs_config_remote.sh <cloudlab-user>` (e.g. `fs435`). This will upload the necessary configurations for the HotStuff Consensus module.

> :warning:  HotStuff is pre-configured to use the server names `us-east-1-0`, `us-east-1-1`, `us-east-1-2`, and `eu-west-1-0`. If you want to change the names of your servers you must also adjust the files `src/scripts/hosts_pg_smr` and `scr/scripts/config_pghs/shard0/hotstuff.gen.conf` accordingly.

   <!-- 3. Open file `config_remote.sh` and edit the following lines to match your Cloudlab credentials:
      - Line 3: `TARGET_DIR="/users/<cloudlab-user>/config/"`
      - Line 14: `rsync -rtuv config <cloudlab-user>@${machine}.<experiment-name>.<project-name>.utah.cloudlab.us:/users/<cloudlab-user>/`
   4. Finally, run `./config_remote.sh` 
   5. This will upload the necessary configurations for the Hotstuff Consensus module to the Cloudlab machines. -->

### **BFTSmart**
   1. Navigate to `Pequin-Artifact/src/scripts`
   2. Build BFT-Smart using `./build_bftsmart.sh`. You only need to do this *once*.
   3. Navigate to `Pequin-Artifact/src/scripts/bftsmart-configs` 
   4. Run `./one_step_config.sh <Local Pequin-Artifact directory> <cloudlab-user> <experiment-name> <project-name> <cluster-domain-name>`
   3. For example: `scripts/bftsmart-configs/one_step_config.sh ../../.. fs435 pequin pequin-pg0 utah.cloudlab.us`
   4. This will upload the necessary configurations for the BFTSmart Conesnsus module to the Cloudlab machines.
      - Troubleshooting: Make sure files `server-hosts` and `client-hosts` in `/src/scripts/bftsmart-configs/` do not contain empty lines at the end

> :warning: Do NOT use `src/scripts/one_step_config.sh` -- specifically use `src/scripts/bftsmart-configs/one_step_config.sh`. The scripts are identical, but for convenience reference different host file configurations.

   <!-- 2. Run `./one_step_config.sh <Local Pequin-Artifact directory> <cloudlab-user> <experiment-name> <project-name> <cluster-domain-name>`
   3. For example: `./one_step_config.sh /home/floriansuri/Research/Projects/Pequin/Pequin-Artifact fs435 pequin pequin-pg0 utah.cloudlab.us`
   4. This will upload the necessary configurations for the BFTSmart Conesnsus module to the Cloudlab machines.
      - Troubleshooting: Make sure files `server-hosts` and `client-hosts` in `/src/scripts/` do not contain empty lines at the end -->


### **Postgres**

> :warning: The experiments reported in the paper were performed with a less clean, manual setup. The new scripts described below should simply processing, but if you run into issues, reach out to us and or follow the old manual instructions

To run an experiment with Postgres you will first need to start a database on your server. 
To configure Postgres to run in primary backup mode you will additionally need to set up a backup replica, and link the primary and backup.

#### Pre-configuring 
First, you must modify scripts and the client connection string according to your `<experiment-name>`, `<cloudlab-user>`, `<cloudlab-cluster>`, and `<project-name>`.

- In `src/store/postgresstore/client.cc` make sure that the connection path is properly set up to match your instantiated experiment, and your primary host name.
- `connection_str = "host="{machine-name}" + experiment_name + {project-name}.{cluster-name}.cloudlab.us" user=pequin_user password=123 dbname=db1 port={port}`. The experiment_name is read in automatically already.
- E.g.: ` connection_str = "host=us-east-1-0." + experiment_name + ".pequin-pg0.utah.cloudlab.us user=pequin_user password=123 dbname=db1 port=5432";`

Additionally, modify the following scripts accordingly to your experiment details and host names.
- `init_postgres.sh`               --> fill in user name, experiment details (name, project, cluster), replica host name. 
- `init_postgres_replicated.sh`    --> fill in user name, experiment details (name, project, cluster), replica host names.  
- `postgres_primary2.sh`           --> replace all instances of `shir` with your own cloudlab user name, and ajdust `replica_servers` to your replica host name.
- `postgres_replica.sh`            --> adjust `primary_ip` according to replica host names.

Finally, in `experiment/scripts/utils/experiment_util.py` adjust the hard coded replica host name according to your setup. Search and adjust this line: `replica_host = "us-east-1-1.pg-smr.pequin-pg0.utah.cloudlab.us"`

#### Uploading helper scripts:
> ⚠️ The following scripts must explicitly be invoked from `Pequin-Artifact/src`, and not from the `scripts` foler directly.
> **Note** If you modify the postgres scripts you will have to re-upload them again!
- If you have not already modify the scripts!
- For unreplicated Postgres, simply invoke `./scripts/init_postgres.sh`
- For primary-backup Postgres invoke `./scripts/init_postgres_replicated.sh` 
- This will upload our replication helper scripts to the primary and backup replicas.
- This needs to be only done once!

During the experiment, our experiment scripts will invoke the uploaded helper scripts automatically to setup the required Postgres environment

**Manual Instructions** (OLD) -- You can ignore this if the above scripts work fine!

   > :warning: **[NOTE]**: These steps have already been completed on our pre-supplied postgres image. However, you will need to adjust the paths in the `postgresql_copy.conf`, `pg_hba_copy.conf` files to match the current cloudlab user, and not fs435.
First, locate the `postgres_service.sh` script (`usr/local/etc/postgres_service.sh`). Then do the following on the machine you intend to run postgres on (e.g. Cloudlab server)
1. Uninstall existing Postgres state: run `./postgres_service.sh -u`
2. If creating a disk iamge, also run `sudo groupadd postgres` and `sudo userdel postgres`
3. Install postgres and initialize a first time: run `./postgres_service.sh -n 1`. This will delete the default main cluster, and create a new one (pgdata) with config files located in `/etc/postgres/12/pgdata`
4. Modify the config files as described here (https://www.bigbinary.com/blog/configure-postgresql-to-allow-remote-connection) in order to enable remote connections
   - Specifically, modify `postgresql.conf` by replacing the line `listen_address = local host` with `listen_address = '*'`
   - And add the following line to the end of `pg_hba.conf`: `host    all             all              0.0.0.0/0                       md5`
   - Each experiment run drops and resets the cluster, which resets also the configs. To avoid making these changes on every run, create copies of the files (`postgresql_copy.conf`, `pg_hba_copy.conf`) and place them in `/usr/local/etc/`. The service script will automatically override the reset configs with the saved copies in each run.



## (3) Using the experiment scripts <a name="scripts"></a>

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
7. `"benchmark_schema_file_path": "/users/fs435/benchmark_data/sql-tpcc-tables-schema.json",`
    - change the user name (fs435) to your <cloudlab-username>
    - note: the file itself depends on which workload you are using
8. `"bftsmart_codebase_dir" : "/users/fs435"`
    - change the user name (fs435) to your <cloudlab-username>
    - this is only applicable to BFTSmart configs

#### **Optional** Modifications 
1. Experiment duration:
   - The provided configs are by default set to run for 60 seconds total, using a warmup and cooldown period of 15 seconds respectively. You may adjust the fields to shorten/lengthen experiments accordingly. For example:
      - "client_experiment_length": 30,
      - "client_ramp_down": 5,
      - "client_ramp_up": 5,
   - For cross-validation purposes shorter experiments likely suffice and save you time (and memory, since output files will be smaller).
   
2. Number of experiments:
   - The provided config files by default run the configured experiment once. If desired, experiments can instead be run several times, allowing us to report the mean throughput/latency as well as standard deviations across the runs. If you want to run the experiment multiple times, you can modify the config entry `num_experiment_runs: 1` to a repetition of your choice, which will automatically run the experiment the specified amount of times, and aggregate the joint statistics.

3. Number of clients:
   - The provided config files by default run a series of experiments for different number of clients. For simple cross-validation purposes this is probably not necessary, and you may want to shorten experiments to include only specific client points.
   - Client settings are defined by the following JSON entries. Each is an array, which allows you to specify a series of clients. Array sizes of the following three params need to match!
      - "client_total": [[30]],
         - "client_total" specifies the upper limit for total client *processes* used
      - "client_processes_per_client_node": [[8]],
         - "client_proccesses_per_client_node" specifies the number of client processes run on each server machine. 
      - "client_threads_per_process": [[1]],
         - "client_threads_per_process" specifies the number of client threads run by each client process.  
   - The *absolute total number* of clients used by an experiment is: 
    - **Total clients** *= max(client_total, num_servers x client_node_per_server x client_processes_per_client_node) *x client_threads_per_process*. 
    - For Pesto (1 shard) "num_servers" = 6, for Peloton (unreplicated) "num_servers" = 1, and for Peloton-SMR "num_servers" = 4.

   - An example client series:
      - "client_total": [[5, 10, 20, 30, 20]],
      - "client_processes_per_client_node": [[8, 8, 8, 8, 8]],
      - "client_threads_per_process": [[1, 1, 1, 1, 2]]

4. Server names:
   - The provided config files use default `server_names`. The name has no meaning in LAN deployments, and serves only as unique identifier (e.g. `us-east-1-0` does not imply where the server will be located). These server names must be consistent with the server names in your deployed CloudLab cluster.
   - If you change the default names, you must also adjust the `server_regions` and `region_rtt_latencies` parameters. Group server names into the region you want to assign them to. The `region_rtt_latencies` values do not matter for LAN deployments; they are placeholders for WAN simulation---see [WAN instructions](#wan-instructions).
  
#### Starting an experiment:
You are ready to start an experiment. The JSON configs we used can be found under `Pequin-Artifact/experiment-configs/<PATH>/<config>.json`. **Note that** all microbenchmark configs are Pesto (Pequin) exclusive.

Run: `python3 <PATH>/Pequin-Artifact/experiment-scripts/run_multiple_experiments.py <PATH>Pequin-Artifact/experiment-configs/<PATH>/<config>.json` and wait!

Optional: To monitor experiment progress you can ssh into a server machine (e.g., us-east-1-0) and run htop. During the experiment run-time the cpus will be loaded (to different degrees depending on contention and client count).
  
   
## (4) Parsing outputs <a name="output"></a>
After the experiment is complete, the scripts will generate an output folder at your specified `base_local_exp_directory`. Each folder is timestamped. 

To parse experiment results you have 2 options:
1. Looking at the `stats.json` file:
   1. Navigate into the timestamped folder, and keep following the timestamped folders until you enter folder `/out`. Open the file `stats.json`. When running multiple client settings, each setting will generate its own internal timestamped folder, with its own `stats.json` file. Multiple runs of the same experiment setting instead will directly be aggregated in a single `stats.json` file.
   2. In the `stats.json` file search for the Json field: `run_stats: ` 
   3. Then, search for the JSON field: `combined:`
   4. Finally, find Throughput measurments under `tput`, Latency measurements under `mean`, and Throughput per Correct client under `tput_s_honest` (**this will exist only for failure experiments**).
2. Looking at generated png plots:
   Alternatively, on your local machine you can navigate to `<time_stamped_folder>/plots/tput-clients.png` and `<time_stamped_folder>/plots/lat-tput.png` to look at the data points directly. Currently however, it shows as "Number of Clients" the number of total client **processes** (i.e. `client_total`) and not the number of **Total clients** specified above. Keep this in mind when viewing output that was generated for experiments with a list of client settings.
   
 Find below, some example screenshots from looking at a provided experiment output from `Pequin-Artifact/sample-output/Pesto/1-Workloads/TPCC`:

 > **NOTE**: We've included a few sample results as illustrative examples. These are *not* the full experiment results. Please refer to section 5 *Running Experiments* to reproduce our results.

   Experiment output folder:

   ![image](https://github.com/user-attachments/assets/e8241b28-77af-42f6-886d-419c63e8e589)

   Contains the results (and configs) from a client series:

   ![image](https://github.com/user-attachments/assets/7e8aa53a-1399-44b5-a938-e31b15476edd)

   The plots folder contains some visualization.

   ![image](https://github.com/user-attachments/assets/89881415-cdf1-4073-8325-720b5e2b520a)

   Additionally, the plots folder contains csv files that automatically parse the Throughput and (mean) Latency for you

   ![image](https://github.com/user-attachments/assets/fa9ff226-6940-4af6-9e78-0f4f5d3f2bf0)


   For details on a specific experiment run, go to one of the experiment folders and inspect `stats.json`. To save space, we've removed all of the client/server logs shown in the picture (only `stats.json` remains)

   ![image](https://github.com/user-attachments/assets/75a526f4-ef72-4e07-be23-0ab8d8981c9b)

   Search for the JSON fields `run_stats` and `combined`. Note: `combined` might not be the first entry within `run_stats` in every config, so double check to get the right data.

   ![image](https://user-images.githubusercontent.com/42611410/129566877-87000119-c43b-4fa2-973a-2a9e571d9351.png)

   Throughput: 

   ![image](https://github.com/user-attachments/assets/7a4c841d-fded-4660-9ddb-0b71401233da)
   
   Latency: 

   ![image](https://github.com/user-attachments/assets/71878eec-8e34-4ded-b8b6-0b5aa98c6abb)


## (5) Reproducing experiment claims 1-by-1 <a name="exp"></a>

Next, we will go over each experiment individually to provide some pointers. All of our experiment configurations can be found under `experiment-configs`.

<!-- **TODO CHANGE ** 
We have included our experiment outputs for easy cross-validation of the claimed througput (and latency) numbers under `/sample-output/ValidatedResults`. 
To directly compare against the numbers reported in our paper please refer to the figures there or the supplied results -- we include rough numbers below as well.  -->

> :warning: Make sure to have set up a CloudLab experiment (with correct disk images matching your local/controllers package dependencies), built all binaries, and created benchmark data before running (see instructions above).

> :warning: Make sure you have correctly set the  `"benchmark_schema_file_path"` as described above!

> **Notice**: When running experiments with load load (i.e. few clients) we observe that the average latency is typically higher than at moderate load (this is the case for all systems). This appears to be a protocol-independent system artifact that we have been unable to resolve so far. CPU and/or network speeds seem to increase under load.

> **Notice**: Some of the systems have matured since the reported results (e.g. undergone minor bugfixes or experienced miscellaneous changes to debug logging). This should have very little impact on performance, but we acknowledge it nonetheless for completeness. The main claims remain consistent.


### **1 - Workloads**:
We report evaluation results for 3 workloads (TPCC, Auctionmark, and Seats) over 7 system setups: 
1. **Pesto** -- our system. A BFT DB.
2. **Pesto-unreplicated** -- Pesto but run with f=0, i.e. a single replica
3. **Peloton** -- an unreplicated SQL DB. Pesto uses Peloton as foundation for its execution engine, so this is provides an apples-to-apples comparison of Pesto's overheads
  - 3.5. **Peloton-signed** -- Peloton, but augmented to reply to clients with signed messages. This illustrates the impact of signatures (an overhead that Pesto incurs)
4. **Peloton-HS** -- Peloton (with reply signatures) run atop a BFT consensus protocol, HotStuff.
5. **Peloton-Smart** -- Peloton (with reply signatures) run atop a BFT consensus protocol, BFTSmart.
6. **Postgres** -- an unreplicated SQL DB of production grade.
7. **Postgres-PB** - Postgres, but run in primary backup mode. Writes are synchronously replicated to a backup.

> **NOTE**: Peloton-HS and Peloton-Smart are not safe systems to run. They do not implement "correct" State Machine Replication (SMR), as they opt to execute requests of different transactions in parallel (for better performance) instead of sequentially. These baselines serve as a generous *upper-bound* on the performance achievable with a simple SMR-based design. Operations from the *same* transaction must still be executed sequentially, however, as this is demanded by the DB interface. Notably though, our system allows independent operations from the same transaction to nonetheless be *issued* by the client in parallel: they can thus proceed through consensus in parallel (thus minimizing latency); only query execution itself must be sequential.

All systems were evaluated using a single shard, but use different replication factors. For f=1, Pesto uses 6 replicas (5f+1), while Peloton-HS and Peloton-Smart use 4 (3f+1). All unreplicated systems use 1 replica. Postgres-PB uses 2 replicas (one primary, one backup).

All systems using signatures (Pesto, Pesto-unreplicated, Peloton-signed, Peloton-HS, Peloton-Smart) are augmented to make use of the reply batching scheme proposed in Basil: replicas may batch together replies to clients and create a single signature to amortize costs. We defer exact details to Basil. We use a varying reply batch size depending on the load; for low load, it is better to not batch to avoid incurring a batch timeout. 
<!-- we used very small batch timer by accident because the unit is *microseconds* and not *miliseconds*. However, due to a libevent artifact, timer granularity is only 4ms, so most of the time our timers are implicitly 4ms. -->

Peak throughput reported in the paper corresponds to maximum attained throughput; latency reported corresponds to latency measured at the "ankle" point, i.e. a bit before latency starts to spike.


 
#### 1. **Pesto**:  

Reproducing our claimed results is straightforward and requires no additional setup besides running the included configs under `/experiment-configs/Pesto/1-Workloads/<workload>/LAN`. 
Reported results were roughly (Tput rounded to int, Lat rounded to 1 decimal point):

    - TPCC: Peak Throughput: ~1.75k tx/s, Ankle Latency: ~17 ms  

        Config file: `/experiment-configs/Pesto/1-Workloads/TPCC/LAN/Pequin-TPCC-SQL-20wh.json`
        Use: `/experiment-configs/Pesto/1-Workloads/TPCC/LAN/Pequin-TPCC-SQL-20wh-low.json` for low load

        For #Clients < 10 we used a reply batch size of b=1; for 10 and above we used b=4.  

        | #Clients    |   1   |   3   |   5   |   10  |   15   |   20   |   30  |   35  |   40  |   45  |
        |-------------|-------|-------|-------|-------|--------|--------|-------|-------|-------|-------|
        | Tput (tx/s) |  90   |  278  |  441  |  850  |  1311  |  1605  |  1784 |  1768 |  1742 |  1705 |
        | Lat (ms)    |  11.3 |  11.1 |  11.6 |  12.1 |  11.8  |  12.8  |  17.3 |  20.4 |  23.7 |  27.2 |
        

    - Auctionmark: Peak Throughput: ~ 3.5k tx/s, Ankle Latency: ~7 ms

        Config file: `/experiment-configs/Pesto/1-Workloads/Auctionmark/Pequin-Auction-sf1.json`
        Use `/experiment-configs/Pesto/1-Workloads/Auctionmark/Pequin-Auction-sf1-low.json` for low load

        For #Clients < 15 we used a reply batch size of b=1; for 15 and above we used b=4.  
    
        | #Clients    |   1   |   5   |   10   |   15   |   20   |   25   |   30   |   35  |
        |-------------|-------|-------|--------|--------|--------|--------|--------|-------|
        | Tput (tx/s) |  182  |  1145 |  2288  |  2846  |  3315  |  3477  |  3568  |  3573 |
        | Lat (ms)    |  5.6  |  4.5  |  4.5   |  5.4   |  6.2   |  7.4   |  8.6   |  10   |
        


    - Seats: Peak Throughput: ~4k tx/s, Ankle Latency: ~7 ms

        Config file: `/experiment-configs/Pesto/1-Workloads/Seats/Pequin-Seats-sf1.json`
        Use `/experiment-configs/Pesto/1-Workloads/Seats/Pequin-Seats-sf1-low.json` for low load

        For #Clients < 15 we used a reply batch size of b=1; for 15 and above we used b=4.  

        | #Clients    |   1   |   5   |   10   |   15   |   20   |   25   |   30   |   40   |   50   |
        |-------------|-------|-------|--------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  159  |  1051 |  2376  |  3029  |  3711  |  3958  |  4055  |  4095  |  4123  |
        | Lat (ms)    |  6.4  |  4.8  |  4.3   |  5.1   |  5.5   |  6.4   |  7.6   |  10    |  11.9  |

    


#### 2. **Pesto-unreplicated**: 

> ⚠️**[Warning]** Do **not** run the unreplicated Pesto configuration in practice. Running with a single replica is **not** BFT tolerant and exists only as an option for microbenchmarking purposes.


    - TPCC: Peak Throughput: ~1.3k tx/s, Ankle Latency: ~12 ms

        Config file: `/experiment-configs/Pesto/1-Workloads/TPCC/LAN/unreplicated/Pequin-unreplicated-TPCC-SQL-20wh.json`
        Use `/experiment-configs/Pesto/1-Workloads/TPCC/LAN/unreplicated/Pequin-unreplicated-TPCC-SQL-20wh-low.json` for low load

        For #Clients < 10 we used a reply batch size of b=1; for 10 and above we used b=4.  

        | #Clients    |   1   |   3   |   5   |   10   |   15   |   20   |   30   |   35   |
        |-----------  |-------|-------|-------|--------|--------|--------|------- |--------|
        | Tput (tx/s) |  132  |  441  |  745  |  1125  |  1337  |  1379  |  1337  |  1328  | 
        | Lat (ms)    |  7.7  |  7    |  6.9  |  9.1   |  11.5  |  14.9  |  23.1  |  27.2  |


    - Auctionmark: Peak Throughput: ~ 3.5k tx/s, Ankle Latency: ~6 ms
    
        Config file: `/experiment-configs/Pesto/1-Workloads/Auctionmark/Pequin-Auction-sf1-unreplicated.json`
        Use `/experiment-configs/Pesto/1-Workloads/Auctionmark/Pequin-Auction-sf1-unreplicated-low.json` for low load

        For #Clients < 20 we used a reply batch size of b=1; for 20 and above we used b=4.  

        | #Clients    |   1   |   5   |   10   |   15   |   20   |   25   |   30   |   35   |   
        |-------------|-------|-------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  218  |  1624 |  2947  |  3353  |  3413  |  3462  |  3487  |  3444  | 
        | Lat (ms)    |  4.7  |  3.2  |  3.5   |  4.6   |  6     |  7.4   |  8.8   |  10.4  |  


    - Seats: Peak Throughput: ~3.8k tx/s, Ankle Latency: ~5 ms

        Config file: `/experiment-configs/Pesto/1-Workloads/Seats/Pequin-unreplicated-Seats-sf1.json`
        Use `/experiment-configs/Pesto/1-Workloads/Seats/Pequin-unreplicated-Seats-sf1-low.json` for low load
        
        For #Clients < 15 we used a reply batch size of b=1; for 15 and above we used b=4.  

        | #Clients    |   1   |   5   |   10   |   15   |   20   |   25   |   30   |   40   |   50   | 
        |-------------|-------|-------|--------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  196  |  1416 |  2898  |  3570  |  3808  |  3790  |  3805  |  3800  |  3827  |
        | Lat (ms)    |  5.2  |  3.6  |  3.5   |  4.3   |  5.4   |  6.7   |  8.1   |  10.8  |  12.9  |



#### 3. **Peloton**: 

> **Note**: Peloton and its Peloton-SMR variants use the same store (`pelotonstore`). You can configure the mode using the config flag `SMR_mode`. Use 0 for unreplicatd Peloton, 1 for Peloton-HS, and 2 for Peloton-Smart. For the latter two, you need to do the respective pre-configuration described above.
<!-- - If SMR_mode = 0, nothing to do
- If == 1 => running HS. Run `scripts/pghs_config_remote.sh`
- If == 2 => running BFTSmart. Run `scripts/build_bftsmart.sh` followed by `scripts/bftsmart-configs/one_step_config ../../.. <cloudlab user> <exp name> <project name> utah.cloudlab.us` -->
   
    - TPCC: Peak Throughput: ~1.8k tx/s, Ankle Latency: ~13 ms

        Config file: `/experiment-configs/Peloton/TPCC/1-LAN/Peloton-TPCC-20wh.json`

        | #Clients    |   1   |   5   |   10   |   20   |   25   |   30   |   40   |   50   |
        |-------------|-------|-------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  135  |  858  |  1301  |  1632  |  1715  |  1777  |  1752  |  1711  
        | Lat (ms)    |  7.6  |  6    |  7.9   |  12.6  |  15    |  17.4  |  23.6  |  30.2 

    
    - Auctionmark: Peak Throughput: ~4.8k tx/s, Ankle Latency: ~6 ms

        Config file: `/experiment-configs/Peloton/Auctionmark/1-LAN/Peloton-Auction.json`

        | #Clients    |   1   |   5   |   10   |   20   |   25   |   30   |   40   |   50   |
        |-------------|-------|-------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  256  |  1848 |  2985  |  4208  |  4582  |  4763  |  4857  |  4851  |  
        | Lat (ms)    |  4    |  2.8  |  3.4   |  4.8   |  5.5   |  6.4   |  8.4   |  10.5  |

    
    - Seats: Peak Throughput: ~5 k tx/s, Ankle Latency: ~6 ms

        Config file: `/experiment-configs/Peloton/Seats/1-LAN/Peloton-Seats.json`

        | #Clients    |   1   |   5   |   10   |   20   |   25   |   30   |   40   |   50   |
        |-------------|-------|-------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  225  |  1688 |  2818  |  4021  |  4427  |  4840  |  4994  |  5036  |
        | Lat (ms)    |  4.5  |  3    |  3.6   |  5.1   |  5.8   |  6.4   |   8.2  |  10.2  |

        
#### 3.5 **Peloton + Reply Sigs**:  
 
This system configuration is not shown in the paper, but we include it here for completeness. You may skip it if just trying to reproduce the results in the paper.
   
    We used a reply batch size of b=4 across all experiments.
   
    - TPCC: Peak Throughput: ~1.5k tx/s, Ankle Latency: ~17 ms

        Config file: `/experiment-configs/Peloton/TPCC/1-LAN/Peloton-Sigs-TPCC-20wh.json`

        | #Clients    |   5   |   10   |   20   |   25   |   30   |      
        |-------------|-------|--------|--------|--------|--------|
        | Tput (tx/s) |  589  |  1030  |  1376  |  1407  |  1502  | 
        | Lat (ms)    |  8.7  |  10    |  15    |  18.3  |  20.6  | 

    
    - Auctionmark: Peak Throughput: ~4.4k tx/s, Ankle Latency: ~8 ms

        Config file: `/experiment-configs/Peloton/Auctionmark/1-LAN/Peloton-Sigs-Auction.json`

        | #Clients    |   5   |   10   |   20   |   25   |   30   |   40   |   60   |
        |-------------|-------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  900  |  2153  |  3321  |  3771  |  4087  |  4323  |  4426  |
        | Lat (ms)    |  5.7  |  4.7   |  6.1   |  6.7   |  7.5   |  9.4   |  13.8  |

    
    - Seats: Peak Throughput: ~4.5 k tx/s, Ankle Latency: ~8 ms

        Config file: `/experiment-configs/Peloton/Seats/1-LAN/Peloton-Sigs-Seats.json`

        | #Clients    |   5   |   10   |   20   |   25   |   30   |   40   |   60   |
        |-------------|-------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  745  |  1851  |  3279  |  3414  |  3951  |  4460  |  4573  |
        | Lat (ms)    |  6.8  |  5.5   |  6.3   |  7.5   |  7.8   |  9.2   |  13.5  |
  
         
#### 4. **Peloton-HS:** 

   Before running Peloton-HS, you must configure Hotstuff using the instructions from section "2) Pre-configurations for Hotstuff", BFTSmart, and Postgres". 
   <!-- Use a batch size of 4 when running TPCC, and 16 for Smallbank and Retwis for optimal results. Note, that you must re-run `src/scripts/remote_remote.sh` **after** updating the batch size and **before** starting an experiment.  -->

   > :warning: Due to its pipelined nature, HotStuff cannot be operated under very low load (i.e. for very few clients), or it will lose progress. This is because the HotStuff module does not implement a batch timer, yet requires 4 batches to be filled to "push" one proposal through the pipeline.

   > **NOTE**: Because HotStuff, by default, implements no batch timer, we augment HotStuff to make use of dynamic (upper bound) batch sizes as used by BFTSmart. We instantiate HotStuff with an upper-bound batch size of 200, but allow batches to form with dynamic smaller size whenever a new pipeline step is ready. This results in optimal latency, which is desirable for contended workloads (e.g. TPCC), while still amortizing costs as much as possible.

   We use a reply batch size of 4 throughout (param `ebatch`).

   > **Note**: The SMR-based systems (Peloton-HS, and Peloton-Smart) have higher latency, and thus require *more* clients to reach high throughput (since clients are closed loop). On contention bottlenecked workloads (such as TPCC) this is unfortunately counterproductive, as more clients create more contention, which results in more aborts, and ultimately less throughput. Peloton-HS thus runs under tension: too few clients, and no progress is made; too many, and they interact destructively. 

    - TPCC: Peak Throughput: ~790 tx/s, Ankle Latency: ~65 ms

        Config file: `/experiment-configs/Peloton/TPCC/1-LAN/Peloton-HS-TPCC-20wh.json`
    
        | #Clients    |   10   |   20   |   30   |   40   |   50   |   60   |   72   |
        |-------------|--------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  223   |  430   |  614   |  684   |  758   |  789   |  763   |
        | Lat (ms)    |  46.4  |  48    |  50.5  |  60.5  |  68.3  |  79    |  99    |

    
    - Auctionmark: Peak Throughput: ~3.3k tx/s, Ankle Latency: ~30 ms

        Config file: `/experiment-configs/Peloton/Auctionmark/1-LAN/Peloton-HS-Auction.json`

        | #Clients    |   15   |   20   |   40   |   60   |   75   |   90   |   100   |   120   |
        |-------------|--------|--------|--------|--------|--------|--------|---------|---------|
        | Tput (tx/s) |  602   |  814   |  1648  |  2280  |  2702  |  2989  |  3147   |  3304   |
        | Lat (ms)    |  25.7  |  25.3  |  24.9  |  25.7  |  28.4  |  30.7  |  31.8   |  37     |  

    
    - Seats: Peak Throughput: ~3.4 k tx/s, Ankle Latency: ~30 ms

        Config file: `/experiment-configs/Peloton/Seats/1-LAN/Peloton-HS-Seats.json`

        | #Clients    |   20   |   30   |   40   |   50   |   60   |   72   |   90   |   100   |   120   | 
        |-------------|--------|--------|--------|--------|--------|--------|--------|---------|---------|
        | Tput (tx/s) |  696   |  1088  |  1494  |  1860  |  2199  |  2496  |  2912  |  3156   |  3420   |
        | Lat (ms)    |  29.6  |  28.4  |  27.5  |  27.6  |  28    |  29.7  |  31.8  |  32.6   |  36.1   |
    
            
      
      
#### 5. **Peloton-Smart**: 

Before running Peloton-Smart, you must configure BFTSmart using the instructions from section "2) Pre-configurations for Hotstuff", BFTSmart, and Postgres". 

> :warning:  Make sure to adjust the `"bftsmart_codebase_dir"` path to reflect your cloudlab-username!
  
You can, but do not need to manually set the batch size for BFTSmart (see optional instruction below). BFTSmart uses dynamically sized batches, and in our experience performs best with an upper bound of 64 and no batch timeout.

We use a reply batch size of 4 throughout (param `ebatch`).

 > **[OPTIONAL]** **If you read, read fully**: To change batch size in BFTSmart navigate to  `src/store/bftsmartstore/library/java-config/system.config` and change line `system.totalordermulticast.maxbatchsize = <batch_size>`. However, explicitly setting this batch size is not necessary, as long as the currently configured `<batch_size>` is `>=` the desired one. This is because BFTSmart performs optimally with a batch timeout of 0, and hence the batch size set *only* dictates an upper bound for consensus batches. Using a larger batch size has no effect. By default our configurations are set to (upper bound) `<batch_size> = 64`.
   
> **[Troubleshooting]**: If you run into any issues (specifically the error: “SSLHandShakeException: No Appropriate Protocol” ) with running BFT-Smart please comment out the following in your `java-11-openjdk-amd64/conf/security/java.security` file: `jdk.tls.disabledAlgorithms=SSLv3, TLSv1, RC4, DES, MD5withRSA, DH keySize < 1024 EC keySize < 224, 3DES_EDE_CBC, anon, NULL`
      
> **Note**: The SMR-based systems (Peloton-HS, and Peloton-Smart) have higher latency, and thus require *more* clients to reach high throughput (since clients are closed loop). On contention bottlenecked workloads (such as TPCC) this is unfortunately counterproductive, as more clients create more contention, which results in more aborts, and ultimately less throughput. 
      
     
    - TPCC: Peak Throughput: ~900 tx/s, Ankle Latency: ~40 ms

        Config file: `/experiment-configs/Peloton/TPCC/1-LAN/Peloton-BFTSMART-TPCC-20wh.json`
    
        | #Clients    |   5   |   10   |   20   |   30   |   40   |   60   |
        |-------------|-------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  178  |  333   |  592   |  785   |  807   |  897   |
        | Lat (ms)    |  28.9 |  30.9  |  34.8  |  39.4  |  51.3  |  69.4  |


    - Auctionmark: Peak Throughput: ~3.3k tx/s, Ankle Latency: ~23 ms

        Config file: `/experiment-configs/Peloton/Auctionmark/1-LAN/Peloton-BFTSMART-Auction.json`


        | #Clients    |   5   |   10   |   15   |   30   |   40   |   60   |   72   |   90   |   100   | 
        |-------------|-------|--------|--------|--------|--------|--------|--------|--------|---------|
        | Tput (tx/s) |  284  |  578   |  839   |  1619  |  2104  |  2821  |  3045  |  3219  |  3271   |
        | Lat (ms)    |  18.2 |  17.8  |  18.4  |  19    |  19.5  |  21.7  |  24.1  |  28.5  |  31.1   |
    

    - Seats: Peak Throughput: ~3.4k tx/s, Ankle Latency: ~23 ms

        Config file: `/experiment-configs/Peloton/Seats/1-LAN/Peloton-BFTSMART-Seats.json`

        | #Clients    |   5   |   10   |   15   |   30   |   40   |   60   |   72   |   90   |   100   |
        |-------------|-------|--------|--------|--------|--------|--------|--------|--------|---------|
        | Tput (tx/s) |  238  |  498   |  743   |  1510  |  2017  |  2842  |  3073  |  3355  |  3425   |
        | Lat (ms)    |  21.5 |  20.6  |  20.8  |  20.4  |  20.4  |  21.7  |  24.1  |  27.6  |  30     |



#### 6. **Postgres**: 

Before running Postgres, you must configure BFTSmart using the instructions from section "2) Pre-configurations for Hotstuff", BFTSmart, and Postgres". 

    - TPCC: Peak Throughput: ~1.8k tx/s, Ankle Latency: ~11 ms

        Config file: `/experiment-configs/Postgres/unreplicated/TPCC/1-LAN/Postgres-TPCC-20wh.json`

        | #Clients    |   1   |   3   |   5   |   8   |   12   |  16   |   24   |   28   |   32   |
        |-------------|-------|-------|-------|-------|--------|-------|--------|--------|--------|
        | Tput (tx/s) |  94   |  447  |  858  |  1325 |  1614  |  1676 |  1781  |  1671  |  1584  |
        | Lat (ms)    |  10.9 |  6.9  |  6    |  6.2  |  7.7   |  10   |  13.8  |  17.2  |  20.8  |


    - Auctionmark: Peak Throughput: ~7k tx/s, Ankle Latency: ~3 ms

        Config file: `/experiment-configs/Postgres/unreplicated/Auctionmark/1-LAN/Postgres-TPCC-20wh.json`

        | #Clients    |   1   |   3   |   5   |   8   |   12   |   16   |   24   |   32   |   40   |
        |-------------|-------|-------|-------|-------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  279  |  1309 |  2625 |  4760 |  6027  |  6530  |  6941  |  6774  |  6648  |
        | Lat (ms)    |  3.7  |  2.3  |  1.9  |  1.7  |  2     |  2.5   |  3.5   |  4.8   |  6.1   |


    - Seats: Peak Throughput: ~8 k tx/s, Ankle Latency: ~3 ms

        Config file: `/experiment-configs/Postgres/unreplicated/TPCC/1-LAN/Postgres-TPCC-20wh.json`

        | #Clients    |   1   |   3   |   5   |   8   |   12   |   16   |   24   |   32   |   40   |
        |-------------|-------|-------|-------|-------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  392  |  1279 |  2884 |  5364 |  6663  |  7444  |  7967  |  7969  |  7768  |
        | Lat (ms)    |  2.6  |   2.4 |  1.8  |  1.5  |  1.8   |  2.2   |  3.1   |  4.1   |  5.3   |



#### 7. **Postgres-PB**: 

Before running Postgres-PB, you must configure BFTSmart using the instructions from section "2) Pre-configurations for Hotstuff", BFTSmart, and Postgres". 

> **NOTE**: Postgres-PB incurs synchronous replication for all write queries. This results in higher latency, and, on contention bottlenecked workloads such as TPCC, in a reduction of throughput.

  <!-- -- replicated postgres has higher latency, so they need more clients to reach higher tput (since closed loop). But more clients = more contention = more latency/less tput
        -- TPCC is a write heavy workload, so the cost of synchronous PB affects it more
        -- contention bottleneck, at high load starts to abort a lot. -->
    
    - TPCC: Peak Throughput: 1257 tx/s, Ankle Latency: ~20ms
    
         Config file: `/experiment-configs/Postgres/primary-backup/PG-TPCC.json`

        | #Clients    |   1   |   5   |   10   |   20   |   30   |   40   |   50   |   60   |   70   |
        |-------------|-------|-------|--------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  81   |  431  |  790   |  1198  |  1238  |  1257  |  1216  |  1099  |  1117  |
        | Lat (ms)    |  12.7 |  11.9 |  13    |  17.2  |  25.1  |  33    |  42.5  |  56.4  |  64.2  |


    - Auctionmark: Peak Throughput: 6084 tx/s, Ankle Latency: ~6ms

         Config file: `/experiment-configs/Postgres/primary-backup/PG-Auction.json`

        | #Clients    |   1   |    5   |   10   |   20   |   30   |   40   |   50   |   60   |   70   |
        |-------------|------ |--------|--------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  190  |  1022  |  2187  |  4529  |  5720  |  6012  |  6084  |  6059  |  6014  |
        | Lat (ms)    |  5.4  |  5     |  4.7   |  4.5   |  5.3   |  6.8   |  8.4   |  10.2  |  11.9  |


    - Seats: Peak Throughput: 7695 tx/s, Ankle Latency ~6ms

        Config file: `/experiment-configs/Postgres/primary-backup/PG-Seats.json`

        | #Clients    |   1   |   5   |   10   |   20   |   30   |   40   |   50   |   60   |   70   |
        |-------------|-------|-------|--------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  233  |  1182 |  2499  |  5447  |  6911  |  7471  |  7695  |  7612  |  7617  |
        | Lat (ms)    |  4.4  |  4.3  |  4.1   |  3.7   |  4.4   |  5.5   |  6.7   |  8.1   |  9.7   |


#### 8. **CockroachDB (CRDB)**: 

> :warning: To run CRDB please switch to branch 'CRDB'. CockroachDB on the branch 'main' is deprecated.


> **NOTE**: CRDB incurs higher query processing overhead compared to Peloton and Postgres. To alleviate it's CPU bottleneck, we allow CRDB to scale horizontally across 6 shards. Shard management in CRDB (i.e. how data is partitioned and where it is placed) is mostly automatic, and may reconfigure itself throughout an experiment. To account for this, we run CRDB experiments with a high warmup time (long enough for the performance to converge). We note, however, that regardless CRDB exhibits fairly volatile performance. We further find, that for low load (few clients) latency is noticeably higher than under load; we thus opted omit results for low load configurations.

> **NOTE**: Client's issuing transactions against CRDB must issue their operations sequentially. This, alongside CRDB's innately slow processing results in high latency for long transactions (e.g. in TPC-C). On a contentded workload such as TPC-C, this in turn results in limited throughput. On the less contended workloads (Auctionmark and SEATS) the effects are less pronounced, and thus CRDB is able to scale to throughput comparable to Peloton and Postgres.

    - TPCC: Peak Throughput: 1033 tx/s, Ankle Latency: ~48ms
    
         Config file: `/experiment-configs/CRDB/CRDB-TPCC-SQL-6` 

        | #Clients    |  25   |  30   |   35   |   40   |   45   |   50   |   60   |
        |-------------|-------|-------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  566  |  694  |  806   |  898   |  954   |  972   |  1033  |
        | Lat (ms)    |  44.4 |  43.4 |  43.6  |  44.7  |  47.4  |  51.8  |  58.4  |


    - Auctionmark: Peak Throughput: 5289 tx/s, Ankle Latency: ~13ms

         Config file: `/experiment-configs/CRDB/CRDB-Auctionmark-SQL-6`

        | #Clients    |  25   |   30   |   35   |   40   |   45   |   55   |   65   |   80   |   90   |
        |-------------|------ |--------|--------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) | 2567  |  3255  |  3778  |  4191  |  4466  |  4863  |  5089  |  5277  |  5289  |
        | Lat (ms)    | 9.5   |  9     |  9     |  9.3   |  9.8   |  11    |  12.4  |  13.8  |  16.4  |


    - Seats: Peak Throughput: 5697 tx/s, Ankle Latency ~13ms

        Config file: `/experiment-configs/CRDB/CRDB-Seats-SQL-6'

        | #Clients    |   20  |   25  |   30   |   35   |   40   |   45   |   50   |   60   |   70   |   85  |  100   |  110   |
        |-------------|-------|-------|--------|--------|--------|--------|--------|--------|--------|-------|--------|--------|
        | Tput (tx/s) |  1884 |  2670 |  3289  |  3806  |  4094  |  4627  |  4934  |  5124  |  5275  |  5472 |  5697  |  5689  |
        | Lat (ms)    |  10.6 |  9.4  |  9.1   |  9.2   |  9.7   |  9.7   |  11.7  |  11.7  |  13.3  |  15.6 |  17.6  |  19.4  |


### **2 - Sharding**:

In addition to Pesto, we also evaluated CockroachDB (CRDB), a popular production-grade distributed database. We do not compare to Peloton and Postgres as they are not easily shardable.
For Pesto we scale to 2 and 3 shards; for CockroachDB we scale to 5 and 9 shards (its peak).

> **[NOTE]** We cannot shard the Peloton baselines. The DB is effectively a blackbox and does not have innate support for sharding. Postgres supports native table partitioning, but not horizontal sharding; one may work around this using the Foreign Data Wrapper (FDW) interface or the Citus extension, but we opted against it for simplicity. CRDB, in contrast, supports automatic sharding which is why we selected it for this experiment. Pesto's commit process supports sharding by design, as it integrates concurrency control and 2PC. Our Pesto prototype, however, does not support distributed queries (i.e. it supports only queries that are satisfied by a single shard); we thus can currently only shard TPC-C, but not Seats or Auctionmark.  

We report below the peak reported throughput. The configuration files referened run a series of client loads to find the peak.

#### 1. Pesto

> :warning: To run a sharded setup you need to make sure that you have allocated enough machines on CloudLab. 1 shard requires 6 servers, 2 shards 12 servers, and 3 shards 18 servers. Make sure your server names match those in the contained configs (param `server_names`).


> :warning: Make sure to *upload* all benchmark data to all servers. When calling `./upload_data_remote -b 'tpcc' -s 2` use the -s flag to pass the number of shards you are using!


    Use the following three configs:
    - 1 shard: `experiment-configs/Pesto/1-Workloads/TPCC/LAN/Pequin-TPCC-SQL-20wh.json` (you do not need to re-run this, it is the same as reported above!!)
    - 2 shards: `experiment-configs/Pesto/1-Workloads/TPCC/LAN/Pequin-TPCC-SQL-20wh-2s.json`
    - 3 shards: `experiment-configs/Pesto/1-Workloads/TPCC/LAN/Pequin-TPCC-SQL-20wh-3s.json`

    Peak results reported were:

    | #Shards     |   1   |   2   |   3   |  
    |-------------|-------|-------|-------|
    | Tput (tx/s) | 1784  | 2934  | 3949  |


#### 2. CRDB

> :warning: To run CRDB please switch to branch 'CRDB'. CockroachDB on the branch 'main' is deprecated.

No additional setup should be necessary to run CRDB. If you run into troubles, please e-mail <larzola@ucsd.edu>.

> :warning: The `server_names` in the following configs are slightly different than our default profile ones. Adjust them according to your experiment!!

> :warning: We've observed CRDB performance to be quite volatile.

    Use the following three configs:
    - 1 shard: `experiment-configs/Cockroach/CRDB-TPCC-SQL-1.json` 
    - 6 shards: `experiment-configs/Cockroach/CRDB-TPCC-SQL-6.json` -- This is the same experiment as above / you do not need to re-run.
    - 9 shards: `experiment-configs/Cockroach/CRDB-TPCC-SQL-9.json`. NOTE: You will need 9 server machines for this. Change your CloudLab expeirment according to the `server_names` in the config.

    Peak results reported were:

    | #Shards     |   1   |   6   |   9   |  
    |-------------|-------|-------|-------|
    | Tput (tx/s) |  400  | 1033  | 1357  |

 > **[NOTE]** CockroachDB is (according to contacts we spoke to) not very optimized for single server performance, and needs to be sharded to be performant.

 > **[NOTE]** CockroachDB (like most databases) only allows for sequential execution of operations within a transaction. This results in high transaction latencies (relative to Pesto) for TPC-C, whose New-Order transaction might issue up to ~45 operations. Pesto, in contrast, can execute many of these operations in parallel, reducing execution latency. Our Peloton baselines strike a midpoint: although execution on the DB itself must be sequential within a transaction, clients do not connect to the DB directly (like for CRDB) but send a message to a replica proxy, which then invokes the DB. This allows clients to send independent operations in parallel, thus sidestepping network and amortizing consensus latency. 


#### 3. Basil

We report the 3 shard result from the [Basil paper](https://www.cs.cornell.edu/~fsp/reports/Suri21Basil.pdf): 4862 tx/s
  



### **3 - Point vs Range Reads **:
Our configs are located under `experiment-configs/Pesto/2-Microbenchmarks/1-Scan-Point`.

Our experiments were run using a single client. The client tries to (in a closed loop) issue a transction that reads a range of <num> rows, either using only point reads, or using the range read protocol. We also evaluate a case in which the read is predicated on a secondary condition (Range-Cond) that applies for only 1 in a 100 rows. 

We used the following configs:
1. Point: `experiment-configs/Pesto/2-Microbenchmarks/1-Scan-Point/Point/<num>.json` 
> **Note**: We evaluated two additional setups, Point-bare and Point-batched, which, respectively, execute a point read only against a KV-store (instead of the SQL backend) -- akin to Basil --, or try to batch replies to amortize signature costs. However, we did not find any meaningful differences so we opted to omit the results. We note further that point reads in this microbenchmark do NOT incur the cost of verifying CommitProofs: because the workload is read only, the only keys read are genesis keys that require no proof in our setup. This implies that in practice, the benefit of range reads is *even bigger* than we claim.

2. Range: `experiment-configs/Pesto/2-Microbenchmarks/1-Scan-Point/Scan/no condition/<num>.json`

3. Range-Cond: `experiment-configs/Pesto/2-Microbenchmarks/1-Scan-Point/Scan/with condition/<num>.json`

The reported results were:

    | #Rows      |   2    |   10   |  100   |  1000  |  10000 |  100000 | 
    |------------|--------|--------|--------|--------|--------|---------|
    | Point      |  3ms   |  4.7   |  25.5  |  222   |  2133  |  21076  |     
    | Range      |  3.3ms |  3.4   |  4.9   |  20    |  128   |  1200   |  
    | Range-Cond |  3.3   |  3.3   |  3.5   |  5.3   |  19.4  |  199    |  
           

### **4 - Stress testing Range Reads**:
In this microbenchmark we simulate artificial inconsistency between replicas to invoke Pesto's snapshot protocol. Configuration files are found under `experiment-configs/Pesto/2-Microbenchmarks/2-Snapshot`

We implement a microbenchmark based on the YCSB framework consisting of $10$ tables, each containing $1M$ keys. Every transaction issues one scan read to a range of size $10$ on one table, and attempts to update all $10$ read rows. We distinguish two workload instantiations: an uncontended uniform access pattern *U*, and a very highly contended Zipfian access pattern *Z* with coefficient 1.1

We simulate two settings:
1) We artificially fail eager execution for *every* transaction -- requiring a snapshot proposal, but no synchronization (since replicas are, in fact, consistent)
2) We ommit/delay application of writes of every transaction at 1/3rd of replicas -- this results in actual inconsistency, and requires both a snapshot proposal and explicit synchronization.
  
<!-- \iffalse
Simulation setup:
At 1/3rd of replicas (2 out of 6) we *drop* application of prepared/committed writes in order to create inconsistency. This results in eager exec failing about 2/3rd of the time (a bit higher for zipf even). When a snapshot is proposed, replicas need to sync on missing data.
Notes:
- with 1/3rd dropping, eager can still succeed if clients contact the 4 replicas that have not dropped.
- we don't want to fail at 1/2 replicas or else lack of prepare means we violate safety (since we allowed commit to go through)
- additionally, it doesnt guarantee that snapshot will include the new tx
- Bonus: Snapshot with optimistic Tx-id is being sent to 6 replicas. This means we might get a reply without sync. To avoid this we simulated by sending to only 5)
- even though we drop the writes, replicas still vote on Prepare, allowing the Tx to possibly finish fast path. 
- An ideal setup would simulate client failures, but we don't want to do this or else we 1) aren't isolating the impact of sync, 2) we have to plot tput/honest
\fi -->


The reported results on the uniform workoad U were:

    - U-Ideal

        Use config `Uniform-Eager-low.json` for #Clients <= 15 (b=2), and `Uniform-Eager.json` for the rest (b=4).

        | #Clients    |   8   |   10   |   15   |   20   |   25   |   30   |
        |-------------|-------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  3085 |  3945  |  5880  |  6911  |  6975  |  7008  |  
        | Lat (ms)    |  2.6  |  2.6   |  2.6   |  3     |  3.7   |  4.4   |

     
    - U-FailEager

        Use config `Uniform-FailEager-low.json` for #Clients <= 16 (b=2), and `Uniform-FailEager.json` for the rest (b=4).

        | #Clients    |   12   |   16   |   20   |   25   |   30   |   35   |
        |-------------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  3404  |  4471  |  5307  |  6140  |  6309  |  6369  |   
        | Lat (ms)    |  3.6   |  3.6   |  3.9   |  4.2   |  4.9   |  5.7   |


    - U-Incon

        Use config `Uniform-Inconsistent.json`

        | #Clients    |   12   |   16   |   20   |   25   |   30   |   35   |   40   | 
        |-------------|--------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  3121  |  4432  |  5400  |  5977  |  6496  |  6658  |  6652  | 
        | Lat (ms)    |  3.9   |  3.7   |  3.8   |  4.3   |  4.8   |  5.4   |  6.2   |



The reported results on the Zipfian workoad Z were:

> **[NOTE]**: The Zipfian workload is highly contended. This can, in tandem with the random exponential backoff, lead to a decent variance in results.

> **[NOTE]**: We've made a small bug fix to range read dependency handling since we ran the numbers reported below. This affect performance slightly for all Zipfian runs (within 5%) as the workload is so heavily contended that there are a lot of dependencies. 

 <!-- Bonus point for Z-Ideal. client 30: (3029.2,10.600514610370176) -->
    - Z-Ideal

        Use config `Zipf-Eager.json`

        | #Clients    |   3   |   5   |   10   |   15   |   20   |   25   |     
        |-------------|-------|-------|--------|--------|--------|--------|
        | Tput (tx/s) |  884  |  1485 |  2319  |  2695  |  2861  |  2884  |   
        | Lat (ms)    |  3.5  |  3.5  |  4.4   |  5.8   |  7.3   |  9.2   |


    - Z-FailEager

        Use config `Zipf-FailEager.json`

        | #Clients    |   3   |   5   |   10   |   15   |   20   |   25   |  
        |-------------|-------|-------|--------|--------|--------|--------|
        | Tput (tx/s) |  621  |  1034 |  1582  |  1823  |  1951  |  1995  |
        | Lat (ms)    |  5    |  5    |  6.5   |  8.5   |  10.7  |  13.1  |


    - Z-Incon

        Use config `Zipf-Inconsistent.json`

        | #Clients    |   3   |   5   |   8   |   12   |   15   |   20   |
        |-------------|-------|-------|-------|--------|--------|--------|
        | Tput (tx/s) |  601  |  996  |  1333 |  1408  |  1498  |  1475  |
        | Lat (ms)    |  5.1  |  5.1  |  6.2  |  9     |  10.5  |  14.4  |


### **5 - Impact of Failure**:
Finally, we evaluate the performance of Pesto under a replica failure. We simulate the replica failure by simply dropping all incoming traffic at one replica.

We distinguish two configurations: 
1) We disable Pesto's commit fast path; commit immediately proceeds to stage 2.
2) We enable the fast path; this results in the timeout expiring before proceeding to stage 2.

> **[NOTE]**: In our config file we set the fast path timeout to 2ms. However, our event library (libevent) by default supports only timer granularity of 4ms. This, in practice, results in timeouts that are 4ms most of the time. Libevent can be configured to use nanosecond timer granularity, but this increases overall overheads and distorts comparison to existing results.
<!-- Note to self: This also explains why the tiny micro-second sized batch timers don't matter, because often it is 4ms -->

> **[NOTE]**: In theory, the fast and slow path can be safely operated in parallel -- in which case both configurations (fast path enabled/disabled) would perform identically. In practice, we opt against it to avoid redundant processing in case we do succeed on the fast path.

Configuration files are found under `experiment-configs/Pesto/2-Microbenchmarks/3-Replica Failure`. 
The results for U-Ideal and Z-Ideal are re-used from the previous experiments. You do not need to re-run them!

The reported results on the uniform workoad U were:

    - U-Ideal 
     
        Same as before. Don't need to re-run!!!

        | #Clients    |   8   |   10   |   15   |   20   |   25   |   30   |
        |-------------|-------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  3085 |  3945  |  5880  |  6911  |  6975  |  7008  |  
        | Lat (ms)    |  2.6  |  2.6   |  2.6   |  3     |  3.7   |  4.4   |
     
    - U-NoFP

        Use config `Uniform-Failure-0.json`

        | #Clients    |   12   |   16   |   21   |   25   |   30   |   35   |   40   |   45   |
        |-------------|--------|--------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  3038  |  4009  |  4567  |  5156  |  5520  |  5816  |  5952  |  6018  |   
        | Lat (ms)    |  4.1   |  4.1   |  4.7   |  5     |  5.6   |  6.2   |  6.9   |  7.7   |

      
    - U-FP

        Use config `Uniform-Failure-2.json`.

         > **Note**: Since latency is higher, we require more clients to reach the same throughput. For #Clients > 45 we used config `Uniform-Failure-2-high.json` to make sure that each client is using its own core. To run this config you will need to instantiate additional client machines on CloudLab (e.g. 2 client machines per server). For simplicity, you can omit this and run only `Uniform-Failure-2.json` which will co-locate 2 clients on the same core. This results in slightly lower performance, but should still be comparable.

        | #Clients    |   25   |   30   |   35   |   40   |   45   |   50   |   60   | 
        |-------------|--------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  3139  |  3801  |  4351  |  4871  |  5355  |  5323  |  5275  | 
        | Lat (ms)    |  8.2   |  8.1   |  8.3   |  8.5   |  8.7   |  9.4   |  12.1  |


> **Note**: We additionally ran an experiment (not shown in the paper) that runs a fault-free experiment with fast path disabled (config `Uniform-NoFP.json`). The resulting performance is pretty much equivalent to running with replica failure (`U-NoFP`).


The reported results on the Zipfian workoad Z were:

> **[NOTE]**: The Zipfian workload is highly contended. This can, in tandem with the random exponential backoff, lead to a decent variance in results.

> **[NOTE]**: We've made a small bug fix to range read dependency handling since we ran the numbers reported below. This affect performance slightly for all Zipfian runs (within 5%) as the workload is so heavily contended that there are a lot of dependencies. 

    - Z-Ideal 
    
        Same as before. Don't need to re-run!!!

        | #Clients    |   3   |   5   |   10   |   15   |   20   |   25   |     
        |-------------|-------|-------|--------|--------|--------|--------|
        | Tput (tx/s) |  884  |  1485 |  2319  |  2695  |  2861  |  2884  |   
        | Lat (ms)    |  3.5  |  3.5  |  4.4   |  5.8   |  7.3   |  9.2   |
       

    - Z-NoFP

        Use config `Zipf-Failure-0.json`

        | #Clients    |   5   |   8   |   12   |   16   |   20   |   25   |   30   |
        |-------------|-------|-------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  1030 |  1544 |  1886  |  2280  |  2521  |  2615  |  2725  |   
        | Lat (ms)    |  4.4  |  4.3  |  4.6   |  5.5   |  6.2   |  7.4   |  8.4   |


    - Z-FP

        Use config `Zipf-Failure-2.json`

        | #Clients    |   10   |   15   |   20   |   25   |   30   |   35   |
        |-------------|--------|--------|--------|--------|--------|--------|
        | Tput (tx/s) |  943   |  1436  |  1625  |  1773  |  2081  |  2116  |   
        | Lat (ms)    |  8.6   |  8.9   |  9.1   |  9.3   |  10.1  |  10.6  |

       

## Other experiments, not in the paper

### Using higher replication factors
By default, all of our experiments use fault tolerance f=1. To use higher replication degrees simply adjust the config parameter `fault_tolerance` and update the `server_names` accordingly. Update also `server_regions` and `region_rtt_latencies` if necessary to match your server naming.

We include an example config for Pesto with f=2 on TPC-C in `experiment-configs/Pesto/1-Workloads/TPCC/LAN/Pequin-TPCC-SQL-20wh-2f.json`. With a higher replication factor performance degrades slightly as coordination (and signature) overheads increase. On TPC-C (20wh, 1 shard) peak throughput drops to ~1660 tx/s (a ~6% decrease).

### WAN instructions
Our experiment setup allows simulation of wide area network (WAN) latencies. 
We opted to omit WAN experiments in the paper because (1) contention bottlnecked workloads (like TPCC) incur very poor performance unless configured with large data sets (which slows down experiment initialization substantially), and (2) the Peloton-SMR prototypes perform even worse as latency rises.

If you are nonetheless interested in using the codebase to simulate WAN experiments, you need to do the following:

1. (Optional) Give your CloudLab servers names that are indicative of their location. Update the `server_names` field in your config file accordingly
2. Configure the `server_regions` parameter. Group server names into the region you want to assign them to.
3. Configure the `region_rtt_latencies` parameter. Specify, for each region, what are the latencies to all other regions.
4. Set `emulate_wan` to true. 

This will allow the experiment scripts to automatically configure `tc` on all servers (and clients) according to the specified latencies.
Note: After running an experiment in WAN mode, the `tc` setup remains active even if you set `emulate_wan` back to false. 
This is an oversight on the experiment scripts, so please keep this in mind if you're switching back to LAN. In that case, you may need to re-start your servers, or manually disable `tc`.

Our `experiment-configs` include experiments for Pesto on three latency setups.

1. LAN: no simulated latency
2. REG: servers are split into a "regional" setup, in which clusters are 10ms apart
3. CON: servers are distributed across North America (contintental) into three locations (US east coast, and 2 locations on US west coast). 
> **Note**: Our experiment configs by default name the third region `eu-west`, but are configured to be the US west coast. Please disregard the naming!

TPCC performance is affected heavily as latency increases as it is contention bound. Auctionmark and Seats are affeted less.


### Running PG_SMR store -- currently deprecated
In addition to layering Peloton atop SMR, we also explored layering Postgres atop SMR. Unfortunately, this results in odd performance behaviors that we have been unable to debug.
You may play around with `pg_SMRstore` if you are interested. However, active support is deprecated.

PG-SMR supports three modes (`SMR_mode`): $0$ runs Postgres via a server proxy, but without SMR. $1$ runs Postgres atop HotStuff, and $2$ runs Postgres atop BFTSmart


## CRDB configuration
> :warning: To run CRDB please switch to branch 'CRDB'. CockroachDB on the branch 'main' is deprecated.
For an indepth look into our CRDB configuration please refer to `src/store/cockroachdb`.

We disable replication (number of replica = 1), but shard the DB across several nodes.
We start each CRDB node using `cockroach start` for a multi-node cluster or `cockroach start-single-node` in the case of a single node. 
We configure connections to CRDB as follows:
    - we use the `--insecure` flag to disable TLS encryption. 
    - we set the listening address and port for incoming connections using `--listen-addr`. 
    - the `--join` flag is used to specify the list of other nodes in the cluster. 
    - `--http-addr` is used to specify the address for the database console UI. 
The last node in the deployment sequence is used to initialize the cluster. 
    - The last node is used to generate the HA Proxy configuration which is used by CRDB for load balancing. 
    - We load balance client traffic to each server by having each client process send traffic to the server node corresponding to `client_id % number_of_servers`.

We run CRDB in memory, and allow it to use full capacity: `--store=type=mem,size=1.0`. 
For best performance, we disable logging except for logs at the FATAL level in the "OPS", "HEALTH", and "SQL_SCHEMA" channels. 

Finally, we set the following parameters:
- we set a lock timeout of 50ms to enhance performance under contention using `ALTER DATABASE defaultdb SET lock_timeout = '50ms';`. 
- we set the minimum bytes for a range to 0 to improve sharding over tables like `Warehouse` in TPCC, which has few rows with few columns but high contention. 
- we set the maximum bytes for a range to 134217728 (128Mb) to avoid overeagerly sharding ranges within a table
 
