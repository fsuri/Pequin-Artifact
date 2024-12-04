#!/bin/bash

# either take as first argument or default to 1
NUM_WAREHOUSES=${1:-1}

cd src/store/benchmark/async/tpcc/
./tpcc_generator --num_warehouses=$NUM_WAREHOUSES > tpcc-1-warehouse
mv tpcc-1-warehouse /usr/local/etc/
