#!/bin/bash

SCRIPT_DIR=$( cd $(dirname $0) && pwd)
nohup python throttle_bandwidth_distribution.py "$@" 1> throttle_bandwidth_distribution_output.txt 2> throttle_bandwidth_distribution_output.txt &

