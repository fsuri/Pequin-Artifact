#!/bin/bash

N=10
OUTPUT_DIR="0_local_test_outputs"

for ((i=1; i<=N; i++)); do
    echo "Iteration: $i"
    ./server-tester.sh
    sleep 1
    ./client-tester.sh
    # check client output
    if grep -Eq LATENCY $OUTPUT_DIR/client-*.out; then
        echo "client-*.out: OK"
    else
        exit 1
    fi
done

echo "Done"
