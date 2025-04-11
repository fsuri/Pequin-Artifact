#!/bin/bash

N=10
OUTPUT_DIR="0_local_test_outputs"

for ((i=1; i<=N; i++)); do
    echo "Iteration: $i"
    ./server-tester.sh
    sleep 1
    ./client-tester.sh
    # check client output
    for file in $OUTPUT_DIR/client-*.out; do
        if [ -f "$file" ]; then
            if grep -q LATENCY $file && ! grep -q mismatch $file; then
                echo "$file: OK"
            else
                exit 1
            fi
        fi
    done
    sleep 0.1
done

echo "Done"