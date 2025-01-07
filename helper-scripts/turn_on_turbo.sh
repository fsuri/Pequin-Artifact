#!/bin/bash

echo "Enabling Turbo Boost"
echo 0 > /sys/devices/system/cpu/intel_pstate/no_turbo
