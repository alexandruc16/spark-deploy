#!/usr/bin/python


# IP address of master VM (string)
master_ip = "10.140.0.166"
# number of slaves to create (integer)
num_slaves = 1
# unique name for the cluster (string)
cluster_name = "aca540"
# templated id to use for the slaves (string)
slave_template = "476"
# temp file to hold slave hostnames (string)
filename = "/tmp/slaves"
# remote username on master e.g. admin or root
remote_username = "alex"
# conf files directory for spark
spark_dir = "/home/" + remote_username + "/spark/spark/conf/"
