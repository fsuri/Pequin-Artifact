#!/bin/bash

BENCHMARK_NAME="tpcc"
GENERATOR_NAME=sql_${BENCHMARK_NAME}_generator
SCALE_FACTOR=1 
NUM_WAREHOUSES=1

MAX_AIRPORTS=-1
K_NEAREST_AIRPORTS=10


while getopts b:s:n:a:k: option; do
case "${option}" in
b) BENCHMARK_NAME=${OPTARG};;
s) SCALE_FACTOR=${OPTARG};;
n) NUM_WAREHOUSES=${OPTARG};;
a) MAX_AIRPORTS=${OPTARG};;
k) K_NEAREST_AIRPORTS=${OPTARG};;
esac;
done

cd store/benchmark/async/sql/$BENCHMARK_NAME

if [ "$BENCHMARK_NAME" = "tpcc" ]; then
	./$GENERATOR_NAME -num_warehouses=$NUM_WAREHOUSES
fi

if [ "$BENCHMARK_NAME" = "seats" ]; then
	./$GENERATOR_NAME --max_airports=$MAX_AIRPORTS --k_nearest_airports=$K_NEAREST_AIRPORTS
fi
 
if [ "$BENCHMARK_NAME" = "auctionmark" ]; then
	./$GENERATOR_NAME
fi

#done
