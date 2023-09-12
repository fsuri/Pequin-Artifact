#!/usr/bin/env bash

CLUSTERID=pgdata
DATA=$(pwd)/tmp-$CLUSTERID
PGV=12
sudo pg_dropcluster --stop $PGV $CLUSTERID
sudo umount $DATA
