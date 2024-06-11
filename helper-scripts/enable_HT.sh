#!/bin/bash
#echo off | sudo tee /sys/devices/system/cpu/smt/control
for i in {8..15}; do
   echo "Enabling logical HT core $i."
   echo 1 > /sys/devices/system/cpu/cpu${i}/online;
done
