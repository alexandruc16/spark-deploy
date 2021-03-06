Spark Benchmarks Installer & Manager
====================================

Summary
---------------------------------------------

This project aims to aid in setting up a cluster via OpenNebula and run
benchmarks for Spark.


Prerequisites
-------------

- an OpenNebula-managed cluster;
- a contextualized virtual machine running Ubuntu 16.04 and with SSH installed.


Description
-----------

The file ``scripts/init.sh`` is the startup script of your VMs.
It does the following:

- contextualizes VM;
- ensures the VM has the correct OpenNebula-assigned IP address;
- configures SSH;
- installs the following packages: ``gcc``, ``make``, ``flex``, ``bison``, ``byacc``, ``git``, ``sbt``, ``maven``, ``python2.7``, ``python-pip``, and the following Python packages: ``paramiko``, ``pyzmq``, ``psutil``
- Installs and configures (or checks for): Oracle JDK v1.8.0, Hadoop v2.7.5, Scala v2.11.12, Spark v2.2.1, HiBench v7.0
- Installs the IBM TCP-DS benchmark: https://github.com/IBM/spark-tpc-ds-performance-test
- Installs Bandwidth Throttler: https://github.com/alexandruc16/bandwidth-throttler (forked from https://github.com/ovedanner/bandwidth-throttler)

A script is included which throttles bandwidths between VMs according to a given distribution and interval at which to generate new values.

Usage
-----

- Instantiate a VM to use as master
- Set appropriate framework configfurations in the ``config_files`` folder
- Verify ``scripts/conf/defaults.py`` settings
- Run ``spark_deploy -m <master_ip>`` to spawn and configure the slave nodes
- Start up the frameworks of your liking
- Run benchmarks
- Graduate :)
