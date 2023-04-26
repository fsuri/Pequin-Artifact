rsync -v -r -e ssh test-1k.csv bcyl2@us-east-1-0.pequin.pequin-pg0.utah.cloudlab.us:/users/bcyl2/indicus/bin/
python experiment-scripts/run_multiple_experiments.py testing/CRDB-config-RW-control.json 
