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
cp /opt/hibench/report/*.report reports/master/
cp /opt/bandwidth-throttler/monitor.* reports/master/
scp slave1.aca540:/opt/bandwidth-throttler/monitor.* reports/slave1/
scp slave2.aca540:/opt/bandwidth-throttler/monitor.* reports/slave2/
scp slave3.aca540:/opt/bandwidth-throttler/monitor.* reports/slave3/
scp slave4.aca540:/opt/bandwidth-throttler/monitor.* reports/slave4/
scp slave5.aca540:/opt/bandwidth-throttler/monitor.* reports/slave5/
scp slave6.aca540:/opt/bandwidth-throttler/monitor.* reports/slave6/
scp slave7.aca540:/opt/bandwidth-throttler/monitor.* reports/slave7/
tar -cvf reports.tar reports/*
