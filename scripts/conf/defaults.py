#!/usr/bin/python

# hostname of master VM
master_hostname = "master.aca540"
# IP address of master VM (string)
master_ip = "10.140.0.133"
# number of slaves to create (integer)
num_slaves = 15
# unique name for the cluster (string)
cluster_name = "aca540"
# templated id to use for the slaves (string)
slave_template = "476"
# temp file to hold slave hostnames (string)
filename = "/tmp/slaves"
# remote username on master e.g. admin or root
remote_username = "aca540"
# hadoop installation directory
hadoop_dir = "/usr/local/hadoop/"
# Spark installation directory
spark_dir = "/usr/local/spark/"
# Hibench installation directory
hibench_dir = "/opt/hibench/"
# OpenNebula Cloud API URL
api_url=""
# OpenNebula Cloud API username
api_user=""
# OpenNebula Cloud API password
api_pass=""
