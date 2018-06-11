#!/bin/sh

# run on master

mkdir reports

cp /opt/hibench/report/*.report reports/
cp /opt/bandwidth-throttler/monitor.* reports/

NODES=$'\n' read -d '' -r -a lines < /usr/local/spark/conf/slaves

for node in "$NODES[@]"
do
    mkdir reports/$node
    scp $node:/opt/bandwidth-throttler/monitor.* reports/$node/
    scp $node:/opt/spark-deploy/scripts/utils/vary.* reports/$node/
fi

tar -cvf reports.tar reports/*

