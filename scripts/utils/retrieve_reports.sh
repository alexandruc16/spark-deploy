#!/bin/sh

# run on master

mkdir reports

cp /opt/hibench/report/*.report reports/

while IFS= read -r node
do
    mkdir reports/$node
    scp $node:/opt/bandwidth-throttler/monitor_* reports/$node/
    scp $node:/opt/spark-deploy/scripts/utils/limits_* reports/$node/
done < "/usr/local/spark/conf/slaves"

tar -cvf reports.tar reports/*

