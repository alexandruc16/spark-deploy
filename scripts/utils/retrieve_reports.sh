#!/bin/sh

# run on master

mkdir reports
mkdir reports/master
mkdir reports/slave1
mkdir reports/slave2
mkdir reports/slave3
mkdir reports/slave4
mkdir reports/slave5
mkdir reports/slave6
mkdir reports/slave7
mkdir reports/slave8
mkdir reports/slave9
mkdir reports/slave10
mkdir reports/slave11
mkdir reports/slave12
mkdir reports/slave13
mkdir reports/slave14
mkdir reports/slave15
cp /opt/hibench/report/*.report reports/master/
cp /opt/bandwidth-throttler/monitor.* reports/master/
scp slave1.aca540:/opt/bandwidth-throttler/monitor.* reports/slave1/
scp slave2.aca540:/opt/bandwidth-throttler/monitor.* reports/slave2/
scp slave3.aca540:/opt/bandwidth-throttler/monitor.* reports/slave3/
scp slave4.aca540:/opt/bandwidth-throttler/monitor.* reports/slave4/
scp slave5.aca540:/opt/bandwidth-throttler/monitor.* reports/slave5/
scp slave6.aca540:/opt/bandwidth-throttler/monitor.* reports/slave6/
scp slave7.aca540:/opt/bandwidth-throttler/monitor.* reports/slave7/
scp slave8.aca540:/opt/bandwidth-throttler/monitor.* reports/slave8/
scp slave9.aca540:/opt/bandwidth-throttler/monitor.* reports/slave9/
scp slave10.aca540:/opt/bandwidth-throttler/monitor.* reports/slave10/
scp slave11.aca540:/opt/bandwidth-throttler/monitor.* reports/slave11/
scp slave12.aca540:/opt/bandwidth-throttler/monitor.* reports/slave12/
scp slave13.aca540:/opt/bandwidth-throttler/monitor.* reports/slave13/
scp slave14.aca540:/opt/bandwidth-throttler/monitor.* reports/slave14/
scp slave15.aca540:/opt/bandwidth-throttler/monitor.* reports/slave15/
cp /opt/spark-deploy/scripts/utils/vary.out reports/master/
scp slave1.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave1/
scp slave2.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave2/
scp slave3.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave3/
scp slave4.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave4/
scp slave5.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave5/
scp slave6.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave6/
scp slave7.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave7/
scp slave8.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave8/
scp slave9.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave9/
scp slave10.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave10/
scp slave11.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave11/
scp slave12.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave12/
scp slave13.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave13/
scp slave14.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave14/
scp slave15.aca540:/opt/spark-deploy/scripts/utils/vary.* reports/slave15/
tar -cvf reports.tar reports/*
