# Running experiments <a name="experiments"></a>
Hurray! You have completed the tedious process of installing the binaries and setting up Cloudlab. Next, we will cover how to run experiments in order to re-produce all results. This is a straightforward but time-consuming process, and importantly requires good network connectivity to upload binaries to the remote machines and download experiment results. Uploading binaries on high speed (e.g university) connections takes a few minutes and needs to be done only once per instantiated cloudlab experiment -- however, if your uplink speed is low it may take (as I have painstakingly experienced in preparing this documentation for you) several hours. Downloading experiment outputs requires a moderate amount of download bandwidth and is usually quite fast. This section is split into 5 subsections: 1) Preparing Benchmarks, 2) Pre-configurations for Hotstuff and BFTSmart, 3) Using the experiment scripts, 4) Parsing outputs, and finally 5) reproducing experiment claims 1-by-1.

Before you proceed, please confirm that the following credentials are accurate:
1. Cloudlab-username `<cloudlab-user>`: e.g. "fs435"
2. Cloudlab experiment name `<experiment-name>`: e.g. "pequin"
3. Cloudlab project name `<project-name`>: e.g. "pequin-pg0"  (May need the "-pg0" extension)

Confirm these by attempting to ssh into a machine you started (on the Utah cluster): `ssh <cloudlab-user>@us-east-1-0.<experiment-name>.<project-name>.utah.cloudlab.us`

## High level experiment checklist
Running experiments involves 5 steps. Refer back to this checklist to stay on track!

> :warning: Make sure to have set up a CloudLab experiment (with correct disk images matching your local/controllers package dependencies) and built all binaries locally before running!.

1. The first step is to generate and upload initial data used by the benchmarks
2. Next, if you're running an SMR-based store (e.g. Peloton-HS or Peloton-Smart), you will need to pre-configure the SMR module. The exact procedure depends on the module you are using.
3. In order to run an experiment, you will need to write (or copy and adjust our pre-supplied) configuration file. This specifies the cluster setup, the benchmark to run, and the parameters of the system.
4. You're ready to run the experiment! Run the experiment script and supply it with your prepared config.
5. Finally, inspect the downloaded experiment run by checking the output data. 

### 1) Preparing Benchmarks

To generate benchmark data simple run the script `src/generate_benchmark_data.sh`, configuring it as follows:
1) specify the benchmark you want to generate, e.g. to run TPC-C use `-b 'tpcc'`
2) specify the benchmark parameters, e.g. to create 20 warehouses for TPC-C use `-n 20`

Generate TPC-C data using: `./generate_benchmark_data -n 20` (tpcc is the default benchmark)
Generate Auctionmark data using: `./generate_benchmark_data -b 'auctionmark'` (using default scale factor)
Generate Seats data using: `./generate_benchmark_data -b 'seats'` (using default scale factor)

Once you created the benchmark data (you can create all data upfront), upload the respective benchmark data to your CloudLab cluster using `src/upload_data_remote`.
Simply specify which benchmark you are uploading, and to how many shards (1, 2 or 3) you are uploading:
E.g. use `./upload_data_remote -b 'tpcc' -s 2` to upload TPC-C data to 2 shards. 
TPC-C and 1 shard are default parameters.

### 2) Pre-configurations for Hotstuff and BFTSmart

When evaluating Peloton-HS and Peloton-Smart you will need to complete the following pre-configuration steps before running an experiment script:

1. **Hotstuff**
   1. Navigate to `Pequin-Artifact/src/scripts`
   2. Run `./batch_size <batch_size>` to configure the internal batch size used by the Hotstuff Consensus module. See sub-section "1-by-1 experiment guide" for what settings to use. The default value is an *upper* cap  of 200. Since we modified Hotstuff to use more efficient, dynamic batch sizes, changing the default batch cap is not necessary.
   3. Run `./pghs_config_remote.sh <cloudlab-user>` (e.g. `fs435`). This will upload the necessary configurations for the HotStuff Consensus module.

> :warning:  HotStuff is pre-configured to use the server names `us-east-1-0`, `us-east-1-1`, `us-east-1-2`, and `eu-west-1-0`. If you want to change the names of your servers you must also adjust the files `src/scripts/hosts_pg_smr` and `scr/scripts/config_pghs/shard0/hotstuff.gen.conf` accordingly.

   <!-- 3. Open file `config_remote.sh` and edit the following lines to match your Cloudlab credentials:
      - Line 3: `TARGET_DIR="/users/<cloudlab-user>/config/"`
      - Line 14: `rsync -rtuv config <cloudlab-user>@${machine}.<experiment-name>.<project-name>.utah.cloudlab.us:/users/<cloudlab-user>/`
   4. Finally, run `./config_remote.sh` 
   5. This will upload the necessary configurations for the Hotstuff Consensus module to the Cloudlab machines. -->

2. **BFTSmart**
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


3. **Postgres**


#### Setting up Postgres (old)

The following steps are necessary to run Postgres.  **TODO**: Use new shir script...
   > :warning: **[NOTE]**: These steps have already been completed on our pre-supplied postgres image. However, you will need to adjust the paths in the `postgresql_copy.conf`, `pg_hba_copy.conf` files to match the current cloudlab user, and not fs435.
First, locate the `postgres_service.sh` script (`src/scripts/postgres_service.sh`). Then do the following on the machine you intend to run postgres on (e.g. Cloudlab server)
1. Uninstall existing Postgres state: run `./postgres_service.sh -u`
2. If creating a disk iamge, also run `sudo groupadd postgres` and `sudo userdel postgres`
3. Install postgres and initialize a first time: run `./postgres_service.sh -n 1`. This will delete the default main cluster, and create a new one (pgdata) with config files located in `/etc/postgres/12/pgdata`
4. Modify the config files as described here (https://www.bigbinary.com/blog/configure-postgresql-to-allow-remote-connection) in order to enable remote connections
   - Specifically, modify `postgresql.conf` by replacing the line `listen_address = local host` with `listen_address = '*'`
   - And add the following line to the end of `pg_hba.conf`: `host    all             all              0.0.0.0/0                       md5`
   - Each experiment run drops and resets the cluster, which resets also the configs. To avoid making these changes on every run, create copies of the files (`postgresql_copy.conf`, `pg_hba_copy.conf`) and place them in `/usr/local/etc/`. The service script will automatically override the reset configs with the saved copies in each run.


### Setting up Postgres-PB and Postgres (new)

WARNING: USE branch 'merged_shir'

To configure Postgres to run in primary backup mode you will additionally need to set up a backup replica, and link the primary and backup.
For this, we have an extra script. Currently, all the setup steps need to be performed manually, and between each experiment run -- we are working on a version that automates this.

#### Pre-configuring 
First, you must modify scripts and the client connection string according to your <experiment-name>, <cloudlab-user>, <cloudlab-cluster>, and <project-name>.

In client.cc make sure that the connection path is properly set up to match your instantiated experiment, and your primary host name.
`connection_str = "host="{machine-name}" + experiment_name + {project-name}.{cluster-name}.cloudlab.us" user=pequin_user password=123 dbname=db1 port={port}`. The experiment_name is read in automatically already.
E.g.: ` connection_str = "host=us-east-1-0." + experiment_name + ".pequin-pg0.utah.cloudlab.us user=pequin_user password=123 dbname=db1 port=5432";`

Additionally, modify the following scripts accordingly to your experiment host names.
- `upload_data_remote.sh`
- `init_postgres_replicated.sh`
- `postgres_primary2.sh`
- `postgres_replica.sh`
- `init_postgres.sh` (only for unreplicated postgres)

#### Uploading helper scripts:
- If you have not already Modify the script `scripts/init_postgres_replicated.sh` according to your <experiment-name>, <cloudlab-user>, <cloudlab-cluster>, and <project-name>.
- Invoke `./scripts/init_postgres_replicated.sh` to upload our replication helper scripts to the primary and backup replicas.
- Likewise, for unreplicated postgres, invoke `./scripts/init_postgres.sh`
- This needs to be only done once!

### Automatic. 

<!-- Outdated manual instructions -->
<!-- #### Configuring replication (before each experiment)
- Next, you will need to active the primary backup setup. To do so, you will have to log into the machines, and manually invoke the helper scripts. You need to do this before *each* experiment. Resetting the cluster setup ensures that Postgres starts from a clean slate each time, and does not retain prior state.
- SSH into the primary machine and run:
    - `./postgres_primary.sh`, followed by `/usr/lib/postgresql/12/bin/pg_ctl -D ~/primary/db start`. And finally run `./postgres_primary2.sh`. (NOTE: packaging these script into a single script somehow did not properly instantiate the service. Please bear with the detour!)
- SSH into the backup machine and run:
    - `./postgres_replica.sh`

Now you are ready to run an experiment via the python script as usual. E.g. `python3 experiment-scripts/run_multiple_experiments.py Postgres-TPCC.json`

#### Tearing down replication (after each experiment):
- After you finish your experiment (and *before* you run another) you must stop the current Postgres cluster
- SSH into the backup machine and run `/usr/lib/postgres/12/bin/pg_ctl -D ~/replica/db stop`
- SSH into the primary machine and run `/usr/lib/postgresql/12/bin/pg_ctl -D ~/primary/db stop` followed by `sudo unmount primary` -->


### 3) Using the experiment scripts

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
   - The provided configs are by default set to run for 60 seconds total, using a warmup and cooldown period of 15 seconds respectively. You may adjust the fields to shorten/lengthen experiments accordingly. For example:
      - "client_experiment_length": 30,
      - "client_ramp_down": 5,
      - "client_ramp_up": 5,
   - For cross-validation purposes shorter experiments likely suffice and save you time (and memory, since output files will be smaller).
   
2. Number of experiments:
   - The provided config files by default run the configured experiment once. If desired, experiments can instead be run several times, allowing us to report the mean throughput/latency as well as standard deviations across the runs. If you want to run the experiment multiple times, you can modify the config entry `num_experiment_runs: 1` to a repetition of your choice, which will automatically run the experiment the specified amount of times, and aggregate the joint statistics.
3. Number of clients:
   - The provided config files by default run an experiment for a single client setting that corresponds to the rough "peak" for throughput. Client settings are defined by the following JSON entries:
      - "client_total": [[30]],
         - "client_total" specifies the upper limit for total client *processes* used
      - "client_processes_per_client_node": [[8]],
         - "client_proccesses_per_client_node" specifies the number of client processes run on each server machine. 
      - "client_threads_per_process": [[1]],
         - "client_threads_per_process" specifies the number of client threads run by each client process.  
   - The *absolute total number* of clients used by an experiment is: **Total clients** *= max(client_total, num_servers x client_node_per_server x client_processes_per_client_node) *x client_threads_per_process*. For example, for Pesto (1 shard) "num_servers" = 6, for Peloton (unreplicated) "num_servers" = 1, and for Peloton-SMR "num_servers" = 4.
   - To determine the peak **Total clients** settings we ran a *series* of client settings for each experiment. For simple cross-validation purposes this is not necessary - If you do however want to, you can run multiple settings automatically by specifying a list of client settings. For example:
      - "client_total": [[5, 10, 20, 30, 20]],
      - "client_processes_per_client_node": [[8, 8, 8, 8, 8]],
      - "client_threads_per_process": [[1, 1, 1, 1, 2]]
   - For convenience, we have included such series (in comments) in all configuration files. To use them, uncomment them (by removing the underscore `_`) and comment out the pre-specified single settings (by adding an underscore `_`).
   - 
#### Starting an experiment:
You are ready to start an experiment. The JSON configs we used can be found under `Pequin-Artifact/experiment-configs/<PATH>/<config>.json`. **Note that** all microbenchmark configs are Pesto (Pequin) exclusive.

Run: `python3 <PATH>/Pequin-Artifact/experiment-scripts/run_multiple_experiments.py <PATH>Pequin-Artifact/experiment-configs/<PATH>/<config>.json` and wait!

Optional: To monitor experiment progress you can ssh into a server machine (us-east-1-0) and run htop. During the experiment run-time the cpus will be loaded (to different degrees depending on contention and client count).
  
   
### 4) Parsing outputs
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

### 5) Reproducing experiment claims 1-by-1

TODO: 
<!-- 1. Confirm profile public --> 
1. Clean up postgres instructions
2. Clean up configs, document exactly what to run  -> Some of them don't have series etc..
3. Clean up results below   --> Show full series? Or just peak points?
Nice to have:
4. Include our example results?  --> Cleanly pick out what to report
5. Parse output example pics, make new
Optional:
6. Add Wan instructions -> how to modify server_names/server_regions
7. Add CRDB instructions (different branch, what to run?)



**TODO CHANGE ** 
We have included our experiment outputs for easy cross-validation of the claimed througput (and latency) numbers under `/sample-output/ValidatedResults`. 
To directly compare against the numbers reported in our paper please refer to the figures there or the supplied results -- we include rough numbers below as well. 

> :warning: Make sure to have set up a CloudLab experiment (with correct disk images matching your local/controllers package dependencies) and built all binaries locally before running (see instructions above).

>: notice: When running with very few clients the average latency is typically higher than at moderate load (this appears to be the case for all systems). This appears to be a protocol-independent system artifact that we have been unable to resolve so far.

>: notice: Some of the systems have matured since the reported results (e.g. undergone minor bugfixes). These should have none, if very little impact on performances, but we acknowledge it nonetheless for completeness. The main claims/takewayas remain consistent.

>: notice: For low number of clients latency on all systems is always a bit higher than under load. Some system artifact we haven't been able to debug -- CPU/network just becomes faster


#### **1 - Workloads**:
We report evaluation results for 3 workloads (TPCC, Smallbank, Retwis) and 4 systems: Tapir (Crash Failure baseline), Basil (our system), TxHotstuff (BFT baseline), and TxBFTSmart (BFT baseline). All systems were evaluated using 3 shards each, but use different replication factors.

   1. **Pesto**: 
 
   Reproducing our claimed results is straightforward and requires no additional setup besides running the included configs under `/experiment-configs/1-Workloads/1.Tapir`. Reported peak results were roughly:
   
   
      1 shard. Batch size: b=4. For low points we use smaller reply batch size (b=1); very small batch timer (2ms = often 4ms with libevent?)
    
            Ankle point: 1784 -17  ==> All relative comparisons/numbers in paper are ankle points!!
      - TPCC: Peak Throughput: ~1.75k tx/s, Stable Latency: ~12 ms  

            Data points used (rounded)

            For 10 and above we used b=4; below we used b=1

            | #Clients    |  1    |   3    |     5  |    10   |     15  |       20       30      35    40     45
            |-------------|-------|--------|--------|---------|---------|----------|
            | Tput (tx/s) |  90   |  278   |  441   |  850    |  1311   |  1605   |   1784 | 1768 | 1742 | 1705
            | Lat (ms)    |  11.3 |  11.1  |  11.6  |  12.1   |  11.8   |  12.8   |   17.3 | 20.4 | 23.7 | 27.2
            

      - Auctionmark: Peak Throughput: ~ 3.5k tx/s, Stable Latency: ~5 ms

          b=4 for 15 and above, below b=1

            | #Clients    | 1      |  5      | 10      |   15        20         25        30       35
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  182   |  1145   |  2288   |  2846    |  3315   |  3477   |   3568 | 3573
            | Lat (ms)    |  5.6   |  4.5    |  4.5    |  5.4     |  6.2    |  7.4    |   8.6  | 10
            


      - Seats: Peak Throughput: ~3.8k tx/s, Stable Latency: ~5 ms

         b=4 for 15 and up 

            | #Clients    |  1        5        10        15        20         25          30      40   50
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  159   |  1051   |  2376   |  3029    |  3711   |  3958   |  4055 |  4095  | 4123
            | Lat (ms)    |  6.4   |  4.8    |  4.3    |  5.1     |  5.5    |  6.4    |   7.6  | 10    | 11.9

    


    2. **Pesto-unreplicated**: 

    > ⚠️**[Warning]** Do **not** run the unreplicated Pesto configuration in practice. Running with a single replica is **not** BFT tolerant and is purely an option for microbenchmarking purposes.
 
   Reproducing our claimed results is straightforward and requires no additional setup besides running the included configs under `/experiment-configs/1-Workloads/1.Tapir`.  Reported peak results were roughly:
   
      - TPCC: Throughput: ~1.3k tx/s, Latency: ~11 ms

          For 10 and above we used b=4; below we used b=1

          | #Clients    |   1    |    3  |    5    |     10       15         20        30       35
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  132   |  441   |  745   |  1125    |  1337   |  1379   |  1337 |  1328  
            | Lat (ms)    |  7.7   |  7    |  6.9    |  9.1     |  11.5    |  14.9    |  23.1  | 27.2 


      - Auctionmark: Throughput: ~ 3.5k tx/s, Latency: ~5 ms
        
           batching for 20 and above

        | #Clients    |       1        5        10        15         20         25      30       35   
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  218   |  1624   |  2947   |  3353    |  3413   |  3462   | 3487   | 3444 | 
            | Lat (ms)    |  4.7   |  3.2    |  3.5    |  4.6     |  6      |  7.4    |   8.8  | 10.4  


      - Seats: Throughput: ~3.7k tx/s, Latency: ~5 ms

            batch for 15 and above 

        | #Clients    |      1         5        10       15        20          25         30    40       50 
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  196   |  1416   |  2898  |  3570    |  3808   |  3790   |  3805 |  3800  | 3827
            | Lat (ms)    |  5.2   |  3.6    |  3.5   |  4.3     |  5.4    |  6.7    |   8.1 | 10.8   | 12.9

    
     1 shard. Batch size: 4.

   3. **Peloton**: 

Peloton and Peloton-SMR variants use the same store (postgresstore)
- If SMR_mode = 0, nothing to do
- If == 1 => running HS. Run `scripts/pghs_config_remote.sh`
- If == 2 => running BFTSmart. Run `scripts/build_bftsmart.sh` followed by `scripts/bftsmart-configs/one_step_config ../../.. <cloudlab user> <exp name> <project name> utah.cloudlab.us`
   
   Use the configurations under `/experiment-configs/1-Workloads/2.Basil`. Reported peak results were roughly:
   
      - TPCC: Throughput: ~1.8k tx/s, Latency: ~10 ms

        | #Clients    |     1   |     5         10       20         25        30       40        50
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  135   |  858   |  1301   |  1632    |  1715   |  1777   |  1752  |  1711  
            | Lat (ms)    |  7.6   |  6    |  7.9    |  12.6     |  15    |  17.4    |   23.6  | 30.2 

    
      - Auctionmark: Throughput: ~4.8k tx/s Latency: ~5 ms

        | #Clients    |      1        5         10        20        25          30       40        50
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  256   |  1848   |  2985   |  4208    |  4582   |  4763   |  4857  |  4851  
            | Lat (ms)    |  4     |  2.8    |  3.4    |  4.8     |  5.5    |  6.4    |   8.4  | 10.5

    
      - Seats: Throughput: ~5 k tx/s, Latency: ~5 ms

        | #Clients    |      1         5        10         20         25        30      40         50
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  225   |  1688   |  2818   |  4021    |  4427   |  4840   |  4994 |  5036
            | Lat (ms)    |  4.5   |  3      |  3.6    |  5.1     |  5.8    |  6.4    |   8.2 | 10.2

        
   > **[NOTE]** We also ran Peloton with reply signatures enabled. TODO: report results? (Not in the paper)

 3.5 **Peloton + Reply Sigs**: 
   
      batch size = 4 for all
   
      - TPCC: Throughput: ~1.8k tx/s, Latency: ~10 ms

        | #Clients    |      5         10       20         25        30      
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  589   |  1030   |  1376    |  1407   |  1502   
            | Lat (ms)    |  8.7   |  10     |  15      |  18.3   |  20.6    | 

    
      - Auctionmark: Throughput: ~4.8k tx/s Latency: ~5 ms

        | #Clients    |       5       10     20     25    30  40    50
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  900    2153    3321    3771       4087    4323   4426
            | Lat (ms)    |  5.7     4.7     6.1   6.7         7.5     9.4     13.8

    
      - Seats: Throughput: ~5 k tx/s, Latency: ~5 ms

        | #Clients    |      5     10        20         25       30      40    60
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  745    1851     3279      3414     3951    4460   4573
            | Lat (ms)    |  6.8     5.5      6.3       7.5    7.8     9.2     13.5 

        
   > **[NOTE]** We also ran Peloton with reply signatures enabled. TODO: report results? (Not in the paper)

         
   4. **Peloton-HS:** 
   
   **TODO: ADAPT HS config. Batch size upper limited now.
   Use the configurations under `/experiment-configs/1-Workloads/3.TxHotstuff`. Before running these configs, you must configure Hotstuff using the instructions from section "1) Pre-configurations for Hotstuff and BFTSmart" (see above). 
   <!-- Use a batch size of 4 when running TPCC, and 16 for Smallbank and Retwis for optimal results. Note, that you must re-run `src/scripts/remote_remote.sh` **after** updating the batch size and **before** starting an experiment.  -->

   >: notice: HotStuff cant be run with too few clients because it is pipelined (e.g. for 1 and 5 clients = no progress). 
    -- smr systems have higher latency, so they need more clients to reach higher tput (since closed loop). But more clients = more contention = more latency/less tput 
    It needs more clients to get anything done... which of course is bad for contention.

    Reply batch: 4. HS batch: upper cap 200, dynamic. Smaller reply batch for low points
   
     Reported peak results were roughly:

     Ankle point:  758, 68
      - TPCC: Throughput: ~768 tx/s, Latency: ~50 ms

        | #Clients    |      10     |  20  |   30  |   40      |  50    |  60    | 72
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  223   |  430   |  614   |  684    |  758   |  789   |  763 
            | Lat (ms)    |  46.4  |  48    |  50.5  |  60.5   |  68.3  |  79    |   99

    
      - Auctionmark: Throughput: ~4.8k tx/s Latency: ~5 ms

        | #Clients    |      15        20      40        60        75          90      100       120
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  602   |  814   |  1648   |  2280    |  2702   |  2989   |  3147 |  3304
            | Lat (ms)    |  25.7  |  25.3  |  24.9   |  25.7    |  28.4   |  30.7   |  31.8 | 37   

    
      - Seats: Throughput: ~5 k tx/s, Latency: ~5 ms

        | #Clients    |      20       30        40        50         60      72         90      100     120 
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  696   |  1088   |  1494   |  1860    |  2199  |  2496   |  2912 |  3156 | 3420
            | Lat (ms)    |  29.6  |  28.4   |  27.5   |  27.6    |  28    |  29.7   | 31.8  |  32.6 | 36.1
    
            
      
   > :warning: **[WARNING]**: Hotstuffs performance can be quite volatile with respect to total number of clients and the batch size specified. Since the Hotstuff protocol uses a pipelined consensus mechanism, it requires at least `batch_size x 4` active client requests per shard at any given time for progress. Using too few clients, and too large of a batch size will get Hotstuff stuck. In turn, using too many total clients will result in contention that is too high, causing exponential backoffs which leads to few active clients, hence slowing down the remaining active clients. These slow downs in turn lead to more contention and aborts, resulting in no throughput. The configs provided by us roughly capture the window of balance that allows for peak throughput. \  
      
   5. **Peloton-Smart**: 
   
   Use the configurations under `/experiment-configs/1-Workloads/4.TxBFTSmart`. Before running these configs, you must configure Hotstuff using the instructions from section "1) Pre-configurations for Hotstuff and BFTSmart" (see above). You can, but do not need to manually set the batch size for BFTSmart (see optional instruction below). Note, that you must re-run `src/scripts/one_step_config.sh` **after** updating the batch size and **before** starting an experiment. 
      
      Reported peak results were roughly:

      Ankle point: 785, 39.4   
      - TPCC: Throughput: ~750 tx/s, Latency: ~30 ms

      -- smr systems have higher latency, so they need more clients to reach higher tput (since closed loop). But more clients = more contention = more latency/less tput

        | #Clients    |     5   |    10        20       30        40       50
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  178   |  333   |  592   |  785    |  807   |  897  
            | Lat (ms)    |  28.9  |  30.9  |  34.8  |  39.4   |  51.3  |  69.4


      - Auctionmark: Throughput: ~4.8k tx/s Latency: ~5 ms

        | #Clients    |      5       10      15       30        40        60         72       90       100 
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  284   | 578    | 839   |  1619   |  2104    |  2821   |  3045   |  3219 |  3271
            | Lat (ms)    |  18.2   | 17.8 |  18.4    |  19    |  19.5  |  21.7   |  24.1   | 28.5  | 31.1
    

      - Seats: Throughput: ~5 k tx/s, Latency: ~5 ms


        | #Clients    |     5        10       15         30        40       60         72      90       100
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  238   |  498   | 743   |  1510   |  2017    |  2842   |  3073   |  3355 |  3425
            | Lat (ms)    |  21.5  |  20.6  |  20.4   |  20.4    |  21.7   |  24.1   |  27.6 | 30


   > **[OPTIONAL NOTE]** **If you read, read fully**: To change batch size in BFTSmart navigate to  `src/store/bftsmartstore/library/java-config/system.config` and change line `system.totalordermulticast.maxbatchsize = <batch_size>`. However, explicitly setting this batch size is not necessary, as long as the currently configured `<batch_size>` is `>=` the desired one. This is because BFTSmart performs optimally with a batch timeout of 0, and hence the batch size set *only* dictates an upper bound for consensus batches. Using a larger batch size has no effect. By default our configurations are set to (upper bound) `<batch_size> = 64`.
   > **[Troubleshooting]**: If you run into any issues (specifically the error: “SSLHandShakeException: No Appropriate Protocol” ) with running BFT-Smart please comment out the following in your `java-11-openjdk-amd64/conf/security/java.security` file: `jdk.tls.disabledAlgorithms=SSLv3, TLSv1, RC4, DES, MD5withRSA, DH keySize < 1024 EC keySize < 224, 3DES_EDE_CBC, anon, NULL`

      
   6. **Postgres**: 
   
    TODO: Use the postgres config instructions
      
      Reported peak results were roughly:

      Ankle point: 785, 39.4   
      - TPCC: Throughput: ~750 tx/s, Latency: ~30 ms

      -- smr systems have higher latency, so they need more clients to reach higher tput (since closed loop). But more clients = more contention = more latency/less tput

        | #Clients    |      1   |   3       5     8       12       16   24      28     32
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  94     447    858   1325      1614   1676   1781   1671   1584
            | Lat (ms)    |  10.9   6.9    6      6.2     7.7     10      13.8   17.2   20.8


      - Auctionmark: Throughput: ~4.8k tx/s Latency: ~5 ms

        | #Clients    |      1        3        5       8        12      16      24       32    40
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  279      1309    2625     4760    6027     6530    6941    6774   6648
            | Lat (ms)    |  3.7       2.3     1.9      1.7     2        2.5     3.5    4.8    6.1
    

      - Seats: Throughput: ~5 k tx/s, Latency: ~5 ms


        | #Clients    |     1        3       5          8        12         16      24      32     40
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  392    1279     2884       5364     6663      7444    7967    7969   7768
            | Lat (ms)    |  2.6     2.4     1.8        1.5       1.8       2.2      3.1    4.1    5.3



     7. **Postgres-PB**: 
   
    TODO: Use the postgres primary backup config instructions
      

      - TPCC:   Ankle point: 1998, 17.2
      -- smr systems have higher latency, so they need more clients to reach higher tput (since closed loop). But more clients = more contention = more latency/less tput

            -- contention bottleneck, at high load starts to abort a lot.
        | #Clients    |      1       5         10       20        30        40    50      60     70
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  81   |  431   |  790   |   1198    |  1238   | 1257    1216  1099    1117
            | Lat (ms)    | 12.7      11.9    13       17.2        25.1     33       42.5  56.4    64.2


      - Auctionmark: Ankle Throughput: ~6084 tx/s Latency: ~8.4

        | #Clients    |     1         5       10          20        30        40         50     60     70
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) | 190   | 1022    | 2187   |  4529   |  5720    |  6012   |  6084   |  6059 |  6014
            | Lat (ms)    |  5.4   | 5    |   4.7    |  4.5    |  5.3  |     6.8   |    8.4   |  10.2  | 11.9
    

      - Seats: Ankle 7695, 6.7


        | #Clients    |    1        5          10        20        30         40       50        60      70
            |-----------|--------|--------|---------|---------|---------|----------|
            | Tput (tx/s) |  233   |  1182   | 2499   |  5447   |  6911    |  7471   |  7695   | 7612 |  7617
            | Lat (ms)    |  4.4      4.3      4.1        3.7      4.4         5.5     6.7        8.1     9.7


2. Sharding


    Note: Need to set sharding in config. + increase number of server names. + need to upload data to all
  Pesto: 1    1784
  Pesto 2     2934
  Pesto 3     3949

  CRDB 1   400
  CRDB 5   1095
  CRDB 9   1357

 > **[NOTE]** CockroachDB is (according to contacts we spoke to) not very optimized for single server performance, and needs to be sharded to be performant.
 > **[NOTE]** CockroachDB (like most databases) only allows for sequential execution of operations within a transaction. This results in high transaction latencies (relative to Pesto) for TPC-C, whose New-Order transaction might issue up to ~45 operations. Pesto, in contrast, can execute many of these operations in parallel, reducing execution latency. Our Peloton baselines strike a midpoint: although execution on the DB itself must be sequential within a transaction, clients do not connect to the DB directly (like for CRDB) but send a message to a replica proxy, which then invokes the DB. This allows clients to send independent operations in parallel, thus sidestepping network and amortizing consensus latency. 

> **[NOTE]** We cannot shard the Peloton baselines. The DB is effectively a blackbox and does not have innate support for sharding. CRDB, in contrast, explicitly supports sharding which is why we selected it for this experiment. Pesto's commit process supports sharding organically, as it integrates concurrency control and 2PC. Our Pesto prototype, however, does not support distributed queries (i.e. it supports only queries that are satisfied by a single shard); we thus can currently only shard TPC-C, but not Seats or Auctionmark.

  Basil-3 (taken from paper, link): 4862

3. Point/Scan

    single client. So no server load.

     |  Type    | r=2    |  r=10  |  r=100  |  r=1000 | r=10000 | r=100000 | 
    |-----------|--------|--------|---------|---------|---------|----------|
    | Point     |  3ms   |  4.7   |  25.5   |  222    |  2133   |  21076   |     
    | Scan      |  3.3ms |  3.4   |  4.9    |  20     |  128    |  1200    |  
    | Scan-Cond |  3.3   |  3.3   |  3.5    |  5.3    |  19.4   |  199     |  
           

4. Inconsistent

    \subsection{Stress testing Range Reads}\label{eval:snapshot}
While range reads offer improved expressivity \changebars{and performance}{, and can drastically reduce latency,} they are not guaranteed to succeed in a single round trip. \changebars{}{Although we exceedingly find replicas to be sufficiently consistent in practice (\S\ref{eval:highlvl}), encountered inconsistencies can result in eager execution to fail, and demand explicit replica synchronization and snapshot materialization to succeed.}To evaluate the worst-case, we stress test \sys{} by \one artificially failing eager execution for \textit{every} transaction -- requiring a snapshot proposal, but no synchronization, and \two artificially simulating inconsistency by ommitting/delaying application of writes of \textit{every} transaction at $\frac{1}{3}$rd of replicas -- requiring both a snapshot proposal and explicit synchronization.

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

We implement a microbenchmark based on the YCSB framework~\cite{Cooper} consisting of $10$ tables, each containing $1M$ keys. Every transaction issues one scan read to a range of size $10$ on one table, and attempts to update all $10$ read rows. We distinguish two workload instantiations: an uncontended \changebars{uniform access pattern \textit{U}}{uniformly random access of tables and ranges (denoted as \\textit{U-<config>})}, and a very highly contended Zipfian access pattern \changebars{\textit{Z} with coefficient 1.1}{ with coefficient $\theta = 1.1$ (denoted as \textit{Z-<config>}).} Figure \ref{fig:snapshot} shows the results.


TODO: ADD DISCUSSION FROM NOTES

Zipfian is highly contended, so with the random exp backoff there can decent variance in results.

    U-Ideal

        | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  3085 |  3945 |  5880  |  6911 |  6975  |  7008   
        | Lat (ms)    |  2.6  |  2.6  |  2.6   |  3    |  3.7   |  4.4 

     
    U-FailEager

          | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  3404 |  4471 |  5307  |  6140 |  6309  |  6369   
        | Lat (ms)    |  3.6  |  3.6  |  3.9   |  4.2  |  4.9   |  5.7 


    U-Incon

          | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  3121 |  4432 |  5400  |  5977  |  6496  |  6658 | 6652   
        | Lat (ms)    |  3.9  |  3.7  |  3.8   |  4.3   |  4.8   |  5.4  | 6.2 


    > **[NOTE]** We've made a small bug fix to range read dependency handling since these numbers. That affects performance slightly for all Zipfian runs (within 5%) since it's so heavily contended that there are a lot of dependencies. 

    Z-Ideal

      | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  884 |  1485 |  2319  | 2695 |  2861 |  2884   
        | Lat (ms)    |  3.5 |  3.5  |  4.4   |  5.8 | 7.3   |  9.2 



    Z-FailEager

      | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  621 |  1034 |  1582  |  1823 |  1951     
        | Lat (ms)    |  5   |  5    |  6.5   |  8.5  |  10.7   


    Z-Incon

      | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  601  |  996  |  1333 |  1408 |  1498     
        | Lat (ms)    |  5.1  |  5.1  |  6.2  |  9    |  10.5   



5. Failure

TODO: ADD DISCUSSION FROM NOTES

We evaluate two configurations: \one \textit{Failure-NoFP} illustrates the effect of a failure when the fast path is disabled.\fs{; this is equivalent to no replica failure (omitted for clarity).} \two \textit{Failure-FP} shows the impact of a failed fast path when using a very conservative timeout of $\approx 4ms$. \fs{I actually set the config to 2ms. However, due to an implementation artifact in our event library~\cite{libevent}\fs{cite!}, timers only have granularity of 4ms, so often it takes 4ms rather than 2ms.}\fs{Note to self: This also explains why the tiny micro-second sized batch timers don't matter, because often it is 4ms}\fs{technically, slow and fast path could be done in parallel, but it would hurt resource util}

    U-Ideal -- Same as before. Don't need to re-run!!!
      | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  3085 |  3945 |  5880  |  6911 |  6975  |  7008   
        | Lat (ms)    |  2.6  |  2.6  |  2.6   |  3    |  3.7   |  4.4 
     
    U-NoFP

      | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  3038 |  4009 |  4567  |  5156 |  5520  |  5816  | 5932 | 6018   
        | Lat (ms)    |  4.1  |  4.1  |  4.7   |  5    |  5.6   |  6.2   | 6.9  | 7.7

      
    U-FP

        > **[NOTE]** We configured the Timeout to be 2ms on paper, but it turns out that the timer precision of the event library we use (libevent) is only 4ms. So in practice, we observe that timeouts are 4ms most of the time. Libevent can be configured to use nanosecond timer granularity, but this increases overall overheads and distorts comparison to existing results.

      | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  3139 |  3801 |  4351  |  4871 |  5355  |  5323 | 5275   
        | Lat (ms)    |  8.2  |  8.1  |  8.3   |  8.5  |  8.7   |  9.4  |  12.1 


    Z-Ideal -- Same as before. Don't need to re-run!!!

      | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  884 |  1485 |  2319  | 2695 |  2861 |  2884   
        | Lat (ms)    |  3.5 |  3.5  |  4.4   |  5.8 | 7.3   |  9.2 
       

    Z-NoFP

      | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  1030 |  1544 |  1886  |  2280 |  2521  |  2615 | 2725   
        | Lat (ms)    |  4.4  |  4.3  |  4.6   |  5.5  |  6.2   |  7.4  | 8.4


    Z-FP

      | #Clients    | c=5    |
        |-----------|--------|--------|---------|---------|---------|----------|
        | Tput (tx/s) |  943 |  1436 |  1625 |  1773 |  2081  |  2116   
        | Lat (ms)    |  8.6  |  8.9 |  9.1  |  9.3  |  10.1  |  10.6 

       

## Other experiments, not in the paper

#### TODO: WAN instructions

set up server names according to region fields.
Specify regions
emulate wan = true

We ran 3 setups for Pesto: LAN, Regional, Contintental.
Reg = 3 clusters, 10ms apart
Con = 3 clusters, east and west coast

Latency hurts tpcc a lot since its contention bound. Hurts Auction and Seats less.
Affects baselines MUCH worse though (since they have high consensus latency) so we opt to omit. Pesto benefit *improves* as latency grows.



#### Running PG_SMR store -- really bad perf
- If SMR_mode = 0, nothing to do
- If == 1 => running HS. Run `scripts/pghs_config_remote.sh`
- If == 2 => running BFTSmart. Run `scripts/build_bftsmart.sh` followed by `scripts/bftsmart-configs/one_step_config ../../.. <cloudlab user> <exp name> <project name> utah.cloudlab.us`








-------------------- OLD :  END DOCUMENT HERE...

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


