#!/bin/bash

kill $(ps aux | grep 'throttle_bandwidth_distribution.py' | awk '{print $2}') 2>/dev/null
