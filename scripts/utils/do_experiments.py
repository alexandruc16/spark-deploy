import argparse
import itertools
import paramiko
import os
import sys
import time
from subprocess import Popen, PIPE
from time import sleep

pkey = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))

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
            
            command += 'nohup python -u /opt/bandwidth-throttler/monitor_bandwidth.py ens3 /opt/bandwidth-throttler/monitor_%s.out /opt/bandwidth-throttler/monitor_%s.in proc 9 1>/dev/null 2>/dev/null &\n' % (file_id, file_id)
            
            if values is not None:
                v = [1024 * x for x in values]
                s = " ".join(map(str, v))
                command += 'nohup python -u /opt/spark-deploy/scripts/utils/vary_bw.py -i 5 -d %s 1>/opt/spark-deploy/scripts/utils/limits_%s.out 2>/opt/spark-deploy/scripts/utils/limits_%s.err &\n' % (s, file_id, file_id)
    
        issue_ssh_commands([worker], command)
        
        
def prepare_hibench_experiment(experiment, exp_folder, workers):
    print("Preparing experiment: " + experiment)
    
    print("Stopping hadoop")
    cmd_res = Popen(["bash", '/usr/local/hadoop/sbin/stop-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
    print(cmd_res)
    
    print("Stopping spark")
    cmd_res = Popen(["bash", '/usr/local/spark/sbin/stop-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
    print(cmd_res)
    
    print("Clearing namenode files")
    cmd_res = Popen(["rm", '-rf', '/usr/local/hadoop/dfs/name'], stdout=PIPE, stderr=PIPE).communicate()[0]
    print(cmd_res)
    cmd_res = Popen(["mkdir", '-P', '/usr/local/hadoop/dfs/name/data'], stdout=PIPE, stderr=PIPE).communicate()[0]
    print(cmd_res)
    
    print("Formating hadoop namenode")
    cmd_res = Popen("echo Y | hdfs namenode -format", shell=True, stdout=PIPE).communicate()[0]
    print(cmd_res)
    
    print("Starting hadoop")
    cmd_res = Popen(["bash", '/usr/local/hadoop/sbin/start-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
    print(cmd_res)
    
    print("Starting spark")
    cmd_res = Popen(["bash", '/usr/local/spark/sbin/start-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
    print(cmd_res)
    
    print("Generating data for experiment: " + experiment)
    prepare_location = os.path.join(exp_folder, 'prepare/prepare.sh')
    cmd_res = Popen(["bash", prepare_location], stdout=PIPE, stderr=PIPE).communicate()[0]
    print(cmd_res)
    
    
def run_hibench_experiment(experiment, exp_folder, workers, times):
    run_location = os.path.join(exp_folder, 'spark/run.sh')
    for i in range(0, times):
        print(experiment + ": Running " + experiment + " #" + str(i + 1))
        cmd_res = Popen(["bash", run_location], stdout=PIPE, stderr=PIPE).communicate()[0]
    
    
def do_hibench_experiment(experiment, exp_folder, workers):
    set_bw_distribution(workers, experiment, 'no_limit', None)
    prepare_hibench_experiment(experiment, exp_folder, workers)
    run_hibench_experiment(experiment, exp_folder, workers, 10)
    
    bandwidth_configs = sorted(BANDWIDTH_CONFIGURATIONS.keys())
    
    for key in bandwidth_configs:
        set_bw_distribution(workers, experiment, key, BANDWIDTH_CONFIGURATIONS[key])
        run_hibench_experiment(experiment, exp_folder, workers, 10)
    
    fnam = '/opt/hibench/report/%s.report' % experiment    
    cmd_res = Popen(["mv", '/opt/hibench/report/hibench.report', fnam], stdout=PIPE, stderr=PIPE).communicate()[0]
    
    
def do_hibench_experiments(workers):
    do_hibench_experiment('sort', '/opt/hibench/bin/workloads/micro/sort', workers)
    do_hibench_experiment('terasort', '/opt/hibench/bin/workloads/micro/terasort', workers)
    do_hibench_experiment('wordcount', '/opt/hibench/bin/workloads/micro/wordcount', workers)
    do_hibench_experiment('kmeans', '/opt/hibench/bin/workloads/ml/kmeans', workers)
    do_hibench_experiment('bayes', '/opt/hibench/bin/workloads/ml/bayes', workers)
    do_hibench_experiment('pagerank', '/opt/hibench/bin/workloads/websearch/pagerank', workers)
    
    print("Finished running experiments")
    set_bw_distribution(workers, None, None, None) # clear bw limits when done
    print("Cleared bandwidth limits and stopped monitors. All done!")
    

def main():
    workers = get_workers()
    do_hibench_experiments(workers)
    set_bw_distribution(workers, None, None)
    

if __name__ == "__main__":
    try:
        main()
    except:
        print("Stopping hadoop")
        cmd_res = Popen(["bash", '/usr/local/hadoop/sbin/stop-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
        
        print("Stopping spark")
        cmd_res = Popen(["bash", '/usr/local/spark/sbin/stop-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]

