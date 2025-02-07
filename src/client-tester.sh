#!/bin/bash

CLIENTS=2
F=0
NUM_GROUPS=1
CONFIG="0_local_test_outputs/configs/shard-r1.config"
CLIENTS_CONFIG="0_local_test_outputs/configs/clients-r${CLIENTS}.config"
POLICY_CONFIG="0_local_test_outputs/configs/policy-grouped.config"
POLICY_FUNCTION="grouped"
PROTOCOL="sintr"
STORE=${PROTOCOL}store
DURATION=5
ZIPF=0.0
NUM_OPS_TX=1
NUM_KEYS_IN_DB=1
KEY_PATH="keys"

STORE_MODE="false"
SQL_BENCH="false"

BENCHMARK="tpcc-sync"

# BENCHMARK="rw-sql"
# FILE_PATH="0_local_test_outputs/rw-sql/rw-sql.json"

#BENCHMARK="tpcc-sql"
#FILE_PATH="store/benchmark/async/sql/tpcc/sql-tpcc-tables-schema.json"

#BENCHMARK="seats-sql"
#FILE_PATH="store/benchmark/async/sql/seats/sql-seats-tables-schema.json"

#BENCHMARK="auctionmark-sql"
#FILE_PATH="store/benchmark/async/sql/auctionmark/sql-auctionmark-tables-schema.json"


while getopts c:f:g:p:s:d:z:o:k:b: option; do
case "${option}" in
c) CLIENTS=${OPTARG};;
f) F=${OPTARG};;
g) NUM_GROUPS=${OPTARG};;
p) CONFIG=${OPTARG};;
s) PROTOCOL=${OPTARG};;
d) DURATION=${OPTARG};;
z) ZIPF=${OPTARG};;
o) NUM_OPS_TX=${OPTARG};;
k) NUM_KEYS_IN_DB=${OPTARG};;
b) BENCHMARK=${OPTARG};;
esac;
done

N=$((5*$F+1))

DEBUG_FILES="store/$STORE/client2client.cc store/$STORE/client.cc store/$STORE/shardclient.cc"
# gdb -ex r -ex bt --args 
# array of process ids
pids=()

echo '[1] Starting new clients'
for i in `seq 1 $((CLIENTS-1))`; do
  #valgrind
  DEBUG=$DEBUG_FILES store/benchmark/async/benchmark --config_path $CONFIG --clients_config_path $CLIENTS_CONFIG --num_groups $NUM_GROUPS \
    --num_shards $NUM_GROUPS \
    --protocol_mode $PROTOCOL --num_keys $NUM_KEYS_IN_DB --benchmark $BENCHMARK --sql_bench=$SQL_BENCH --data_file_path $FILE_PATH --num_ops_txn $NUM_OPS_TX \
    --exp_duration $DURATION --client_id $i --num_client_hosts $CLIENTS --warmup_secs 0 --cooldown_secs 0 \
    --key_selector zipf --zipf_coefficient $ZIPF --indicus_key_path $KEY_PATH \
    --store_mode=$STORE_MODE --indicus_hash_digest=true --indicus_verify_deps=false --sintr_debug_endorse_check=false \
    --sintr_max_val_threads=2 --sintr_policy_config_path $POLICY_CONFIG  --sintr_policy_function_name $POLICY_FUNCTION \
    --sintr_read_include_policy=0 --indicus_no_fallback=false &> ./0_local_test_outputs/client-$i.out &
  pids+=($!)
done;

#valgrind
DEBUG=$DEBUG_FILES store/benchmark/async/benchmark --config_path $CONFIG --clients_config_path $CLIENTS_CONFIG --num_groups $NUM_GROUPS \
  --num_shards $NUM_GROUPS --protocol_mode $PROTOCOL --num_keys $NUM_KEYS_IN_DB --benchmark $BENCHMARK --sql_bench=$SQL_BENCH --data_file_path $FILE_PATH \
  --num_ops_txn $NUM_OPS_TX --exp_duration $DURATION --client_id 0 --num_client_hosts $CLIENTS --warmup_secs 0 \
  --cooldown_secs 0 --key_selector zipf --zipf_coefficient $ZIPF \
  --stats_file "stats-0.json" --indicus_key_path $KEY_PATH \
  --store_mode=$STORE_MODE --indicus_hash_digest=true --indicus_verify_deps=false --sintr_debug_endorse_check=false \
  --sintr_max_val_threads=2 --sintr_policy_config_path $POLICY_CONFIG --sintr_policy_function_name $POLICY_FUNCTION \
  --sintr_read_include_policy=0 --indicus_no_fallback=false &> ./0_local_test_outputs/client-0.out &
pids+=($!)


sleep $((DURATION+4))
echo '[2] Shutting down possibly open servers and clients'
killall store/benchmark/async/benchmark
#callgrind_control --dump
killall store/server

# sometimes killall doesn't work so make sure we stop all processes
for pid in "${pids[@]}"; do
  kill $pid
done
