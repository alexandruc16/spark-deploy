#!/bin/bash
cp /opt/spark-sql-perf/target/scala-2.11/spark-sql-perf-assembly-0.5.0-SNAPSHOT.jar /opt/spark-deploy/scripts/utils/tpcds/spark-sql-perf/lib/
sbt clean package
