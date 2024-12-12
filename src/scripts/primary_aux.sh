#!/usr/bin/env bash

./postgres_primary.sh
/usr/lib/postgresql/12/bin/pg_ctl -D ~/primary/db start >/dev/null
./postgres_primary2.sh 