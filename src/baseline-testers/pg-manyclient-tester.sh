#!/bin/bash

CLIENTS=10
F=1
NUM_GROUPS=1
CONFIG="0_local_test_outputs/configs/shard-r6.config"
PROTOCOL="postgres"
STORE=${PROTOCOL}store
DURATION=10
ZIPF=0.0
NUM_OPS_TX=2
NUM_KEYS_IN_DB=1
KEY_PATH="keys"
BENCHMARK="tpcch-sql"

SQL_BENCH="true"
FILE_PATH="sql-tpcc-tables-schema.json"
#"0_local_test_outputs/seats-sql/seats-sql.json"
#"0_local_test_outputs/kv_example/kv-tables-schema.json"
USR_NAME="tostitos"
CONN_STR="postgres://$USR_NAME:postgres@localhost:5432/tpcch"

while getopts c:f:g:cpath:p:d:z:num_ops:num_keys:b: option; do
case "${option}" in
c) CLIENTS=${OPTARG};;
f) F=${OPTARG};;
g) NUM_GROUPS=${OPTARG};;
cpath) CONFIG=${OPTARG};;
p) PROTOCOL=${OPTARG};;
d) DURATION=${OPTARG};;
z) ZIPF=${OPTARG};;
num_ops) NUM_OPS_TX=${OPTARG};;
num_keys) NUM_KEYS_IN_DB=${OPTARG};;
b) BENCHMARK=${OPTARG};;
esac;
done

N=$((5*$F+1))

echo '[1] Starting new clients'
for i in `seq 1 $((CLIENTS-1))`; do
  #valgrind
  store/benchmark/async/benchmark --config_path $CONFIG --num_groups $NUM_GROUPS \
    --num_shards $NUM_GROUPS \
    --protocol_mode $PROTOCOL --num_keys $NUM_KEYS_IN_DB --benchmark $BENCHMARK --sql_bench=$SQL_BENCH  --num_ops_txn $NUM_OPS_TX \
    --exp_duration $DURATION --client_id $i --num_client_hosts $CLIENTS --warmup_secs 0 --cooldown_secs 0 \
    --key_selector zipf --zipf_coefficient $ZIPF --stats_file "stats-$i.json" --connection_str $CONN_STR --indicus_key_path $KEY_PATH &> ./0_local_test_outputs/client-$i.out &
done;

#valgrind

sleep $((DURATION+4))
echo '[2] Shutting down possibly open servers and clients'
killall store/benchmark/async/benchmark
killall store/server