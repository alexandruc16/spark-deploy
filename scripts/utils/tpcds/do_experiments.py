import argparse
import itertools
import paramiko
import os
import sys
import time
from subprocess import Popen, PIPE
from time import sleep

pkey = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))

NO_LIMIT_CONFIGURATION = [1000.000, 1000.000, 1000.000, 1000.000, 1000.000]
BANDWIDTH_CONFIGURATIONS = {
    'A': [60.344, 149.999, 263.793, 384.482, 653.448],
    'B': [308.620, 503.448, 646.551, 789.655, 991.379],
    'C': [555.172, 841.379, 901.724, 934.482, 946.551],
    'D': [112.068, 137.931, 170.689, 199.999, 298.275],
    'E': [774.137, 824.137, 851.724, 855.172, 858.620],
    'F': [268.965, 729.310, 777.586, 820.689, 925.862],
    'G': [334.482, 529.310, 537.931, 600.000, 624.137],
    'H': [136.206, 425.862, 525.862, 660.344, 998.275]
}


def get_workers():
    result = []
    
    with open('/usr/local/spark/conf/slaves') as f:
        result = [line.rstrip('\n') for line in f]
        
    return result


def issue_ssh_commands(slaves_list, commands, remote_username='aca540', master_ip=None):
    timeout = 5
    
    if not master_ip is None:
        slaves_list.insert(0, master_ip)
            
    for ip in slaves_list:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username=remote_username, pkey=pkey)
            channel = ssh.invoke_shell()
            stdin = channel.makefile('wb')
            stdout = channel.makefile('rb')

            stdin.write(commands)
            endtime = time.time() + timeout
            
            while not stdout.channel.eof_received:
                sleep(1)
                
                if time.time() > endtime:
                    stdout.channel.close()
                    break
            
            #print(stdout.read())

            stdout.close()
            stdin.close()
            channel.close()
            ssh.close()
        
        except:
            print("Error occurred while issuing SSH commands to " + ip)
            print(commands)
            raise
                
    
def set_bandwidths(workers, values):
    for i in range(0, len(workers)):
        worker = workers[i]
        command = 'sudo bash /opt/wondershaper/wondershaper -c -a ens3\n'
        
        if values is not None:
            value = 1024 * values[i]
            command += 'sudo bash /opt/wondershaper/wondershaper -a ens3 -u %d -d %d\n' % (value, value)
    
        issue_ssh_commands([worker], command)
        
        
def set_bw_distribution(workers, experiment=None, config_key=None, values=None):
    for i in range(0, len(workers)):
        worker = workers[i]
        command = 'sudo pkill -f vary_bw.py\n'
        command += 'sudo pkill -f monitor_bandwidth.py\n'
        
        if experiment is not None and config_key is not None:
            file_id = "%s_%s" % (experiment, config_key)
            command += 'rm -rf /opt/bandwidth-throttler/monitor_%s.in\n' % file_id
            command += 'rm -rf /opt/bandwidth-throttler/monitor_%s.out\n' % file_id
            command += 'nohup python -u /opt/bandwidth-throttler/monitor_bandwidth.py ens3 /opt/bandwidth-throttler/monitor_%s.out /opt/bandwidth-throttler/monitor_%s.in proc 9 1>/dev/null 2>/dev/null &\n' % (file_id, file_id)
            
            if values is not None:
                v = [1024 * x for x in values]
                s = " ".join(map(str, v))
                command += 'rm -rf /opt/spark-deploy/scripts/utils/limits_%s.out\n' % file_id
                command += 'rm -rf /opt/spark-deploy/scripts/utils/limits_%s.err\n' % file_id
                command += 'nohup python -u /opt/spark-deploy/scripts/utils/vary_bw.py -i 5 -d %s 1>/opt/spark-deploy/scripts/utils/limits_%s.out 2>/opt/spark-deploy/scripts/utils/limits_%s.err &\n' % (s, file_id, file_id)

        issue_ssh_commands([worker], command)


def stop_spark():
    print("Stopping spark")
    try:
        cmd_res = Popen(["bash", '/usr/local/spark/sbin/stop-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
    except Exception as e:
        print(e)


def start_spark():
    print("Starting spark")
    try:
        cmd_res = Popen(["bash", '/usr/local/spark/sbin/start-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
    except Exception as e:
        print(e)


def stop_hadoop():
    print("Stopping Hadoop")
    try:
        cmd_res = Popen(["bash", '/usr/local/hadoop/sbin/stop-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
    except Exception as e:
        print(e)


def start_hadoop():
    print("Starting hadoop")
    try:
        cmd_res = Popen(["bash", '/usr/local/hadoop/sbin/start-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
    except Exception as e:
        print(e)


def stop_cluster():
    stop_spark()
    stop_hadoop()


def start_cluster():
    start_hadoop()
    start_spark()


def run_tpcds_experiment(workers, bw_config):
    print("Running TPCDS experiment: " + bw_config)
    try:
        cmd_res = Popen(
            "spark-submit --class \"RunTPCDS\" --executor-cores 4 --num executors 32 --executor-memory 10g --master yarn-client --jars /opt/spark-sql-perf/target/scala-2.11/spark-sql-perf-assembly-0.5.0-SNAPSHOT.jar /opt/spark-deploy/scripts/utils/tpcds/spark-sql-perf/target/scala-2.11/tpcds-experiments_2.11-1.0.jar",
            shell=True, stdout=PIPE).communicate()[0]
        print(cmd_res)
    except Exception as e:
        print(e)

    set_bw_distribution(workers, None, None, None)  # clear bw limits when done

    try:
        print("Saving experiment results to HDFS")
        cmd_res = Popen(
            "spark-submit --class \"RetrieveResults\" --executor-cores 4 --num executors 32 --executor-memory 10g --master yarn-client --jars /opt/spark-sql-perf/target/scala-2.11/spark-sql-perf-assembly-0.5.0-SNAPSHOT.jar /opt/spark-deploy/scripts/utils/tpcds/spark-sql-perf/target/scala-2.11/tpcds-experiments_2.11-1.0.jar",
            shell=True, stdout=PIPE).communicate()[0]
        print(cmd_res)
        out_location = "/opt/spark-deploy/scripts/utils/reports/%s" % bw_config
        print("Retrieving results from HDFS")
        cmd_res = Popen(["hdfs", "dfs", "-get", "/results", out_location], stdout=PIPE, stderr=PIPE).communicate()[0]
        print(cmd_res)
        cmd_res = Popen(["hdfs", "dfs", "-rmr", "/spark" "/results"], stdout=PIPE, stderr=PIPE).communicate()[0]
        print(cmd_res)
    except Exception as e:
        print(e)


def prepare_tpcds_experiments():
    print("Preparing TPCDS data")

    try:
        cmd_res = Popen(
            "spark-submit --class \"PrepareTPCDS\" --executor-cores 4 --num executors 32 --executor-memory 10g --master yarn-client --jars /opt/spark-sql-perf/target/scala-2.11/spark-sql-perf-assembly-0.5.0-SNAPSHOT.jar /opt/spark-deploy/scripts/utils/tpcds/spark-sql-perf/target/scala-2.11/tpcds-experiments_2.11-1.0.jar",
            shell=True, stdout=PIPE).communicate()[0]
        print(cmd_res)
    except Exception as e:
        print(e)


def do_tpcds_experiments(workers):
    prepare_tpcds_experiments()
    set_bw_distribution(workers, "tpcds", "no_limit", NO_LIMIT_CONFIGURATION)
    run_tpcds_experiment(workers, "no_limit")
    bandwidth_configs = sorted(BANDWIDTH_CONFIGURATIONS.keys())

    for key in bandwidth_configs:
        set_bw_distribution(workers, "tpcds", key, BANDWIDTH_CONFIGURATIONS[key])
        run_tpcds_experiment(workers, key)

    print("Finished running TPC-DS experiments")


def main():
    workers = get_workers()
    do_tpcds_experiments(workers)
    print("Program ended")
    

if __name__ == "__main__":
    try:
        main()
    except:
        print("Stopping hadoop")
        cmd_res = Popen(["bash", '/usr/local/hadoop/sbin/stop-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
        
        print("Stopping spark")
        cmd_res = Popen(["bash", '/usr/local/spark/sbin/stop-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]

