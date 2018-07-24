#!/bin/sh

# run this from the hibench directory to set everything up
mvn -Psparkbench -Dspark=2.2 -Dscala=2.11 clean package
bash /opt/hibench/bin/workloads/micro/sort/prepare/prepare.sh
bash /opt/hibench/bin/workloads/micro/terasort/prepare/prepare.sh
bash /opt/hibench/bin/workloads/micro/bayes/prepare/prepare.sh
bash /opt/hibench/bin/workloads/micro/kmeans/prepare/prepare.sh
bash /opt/hibench/bin/workloads/ml/bayes/prepare/prepare.sh
bash /opt/hibench/bin/workloads/websearch/pagerank/prepare/prepare.sh

