#!/bin/bash
#echo off | sudo tee /sys/devices/system/cpu/smt/control
for i in {8..15}; do
   echo "Disabling logical HT core $i."
   echo 0 > /sys/devices/system/cpu/cpu${i}/online;
done
