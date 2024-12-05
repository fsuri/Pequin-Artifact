# Running postgres experiment 

For replicated postgres: Cloudlab experiments uses 4 machines (only 3 are actually used) - 2 servers: one serves as primary and one as replication. 1 client machine is used.

On your local machine (steps 1-3):
1. Go to “src” folder
a. `cd /Pequin-Artifact/src` (or wherever your code is)
2. Generate the benchmark data and upload it to the experiment server.
a. `./generate_benchmark_data.sh -n 20` (args change based on benchmark)
b. `./upload_data_remote.sh`
3. Copy postgres set-up scripts to the machines. Scripts should be configured with experiment name, user name, cluster, and project name. List of servers may also need to be adjusted. *see list below for files that should be adjusted.
a. For replicated pg: `./scripts/init_postgres_replicated.sh`
OR
b. For non-replicated pg: `./scripts/init_postgres.sh`

4. `python3 ~/Pesto/Pequin-Artifact/experiment-scripts/run_multiple_experiments.py "/home/sc3348/Pesto/Pequin-Artifact/testing/sql/PG-TPCC.json"`
In the config you run, set  "pg_replicated": true/false according to the desired behavior.

Notes:
1. On “~/Pequin-Artifact/src/store/postgresstore/client.cc” - the connection path should be adjusted to match the experiment information.
I.e.:
 `connection_str = "host={us-east-1-0}." + experiment_name + ".{pequin-pg0}.{utah}.cloudlab.us user=pequin_user password=123 dbname=db1 port="+port;`
For other benchmarks steps are exactly the same, changes are required in steps 2 and 4 (generating information and experiment configuration file).
Experiment config file should be modified with the relative information.

# General scripts to adjust:
- Upload_data_remote.sh
# Replicated scripts to adjust:
- Init_postgres_replicated.sh
- Postgres_primary2.sh
- postgres_replica.sh
# Non-replicated scripts to adjust:
- init_postgres.sh