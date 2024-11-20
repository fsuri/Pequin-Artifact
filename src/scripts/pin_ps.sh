#!/usr/bin/env bash


# pgrep -u postgres # returns the list of postgres processes,



PIDS=$(pgrep -u postgres) 
for PID in $PIDS; do
    sudo taskset -p -p 0x3FC $PID
done