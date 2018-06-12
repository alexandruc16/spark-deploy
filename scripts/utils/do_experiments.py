import argparse
import itertools
import paramiko
import os
import sys
import time
from subprocess import Popen, PIPE
from time import sleep

pkey = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))

A = [60.344, 149.999, 263.793, 384.482, 653.448]
B = [308.620, 503.448, 646.551, 789.655, 991.379]
C = [555.172, 841.379, 901.724, 934.482, 946.551]
D = [112.068, 137.931, 170.689, 199.999, 298.275]
E = [774.137, 824.137, 851.724, 855.172, 858.620]
F = [268.965, 729.310, 777.586, 820.689, 925.862]
G = [334.482, 529.310, 537.931, 600.000, 624.137]
H = [136.206, 425.862, 525.862, 660.344, 998.275]

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
        
        
def set_bw_distribution(workers, config_key, values):
    for i in range(0, len(workers)):
        worker = workers[i]
        command = 'sudo pkill -f vary_bw.py\n'
        command += 'sudo pkill -f monitor_bandwidth.py\n'
        
        if config_key is not None:
            command += 'nohup python -u /opt/bandwidth-throttler/monitor_bandwidth.py ens3 /opt/bandwidth-throttler/monitor_%s.out /opt/bandwidth-throttler/monitor_%s.in proc 9 1>/dev/null 2>/dev/null &\n' % (config_key, config_key)
        
        if values is not None:
            s = " ".join(map(str, values))
            command += 'nohup python -u /opt/spark-deploy/scripts/utils/vary_bw.py -i 5 -d %s 1>/opt/spark-deploy/scripts/utils/limits_%s.out 2>/opt/spark-deploy/scripts/utils/limits_%s.err &\n' % (s, config_key, config_key)
    
        issue_ssh_commands([worker], command)
    

def run_experiments(workers, values=None, typ=None):
    #set_bandwidths(workers, values)
    
    if typ is None:
        typ = "no_limit"
        
    set_bw_distribution(workers, typ, values)
    
    for i in range(0, 10):
        print(typ + ": Running sort #" + str(i + 1))
        cmd_res = Popen(["bash", '/opt/hibench/bin/workloads/micro/sort/spark/run.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
        
    for i in range(0, 10):
        print(typ + ": Running terasort #" + str(i + 1))
        cmd_res = Popen(["bash", '/opt/hibench/bin/workloads/micro/terasort/spark/run.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
        
    for i in range(0, 10):
        print(typ + ": Running wordcount #" + str(i + 1))
        cmd_res = Popen(["bash", '/opt/hibench/bin/workloads/micro/wordcount/spark/run.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
        
    for i in range(0, 10):
        print(typ + ": Running bayes #" + str(i + 1))
        cmd_res = Popen(["bash", '/opt/hibench/bin/workloads/ml/bayes/spark/run.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
        
    for i in range(0, 10):
        print(typ + ": Running kmeans #" + str(i + 1))
        cmd_res = Popen(["bash", '/opt/hibench/bin/workloads/ml/kmeans/spark/run.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
        
    for i in range(0, 10):
        print(typ + ": Running pagerank #" + str(i + 1))
        cmd_res = Popen(["bash", '/opt/hibench/bin/workloads/websearch/pagerank/spark/run.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
    
    fnam = '/opt/hibench/report/%s.report' % typ
    cmd_res = Popen(["mv", '/opt/hibench/report/hibench.report', fnam], stdout=PIPE, stderr=PIPE).communicate()[0]
           

def main():
    workers = get_workers()
    run_experiments(workers)
    run_experiments(workers, A, 'A')
    run_experiments(workers, B, 'B')
    run_experiments(workers, C, 'C')
    run_experiments(workers, D, 'D')
    run_experiments(workers, E, 'E')
    run_experiments(workers, F, 'F')
    run_experiments(workers, G, 'G')
    run_experiments(workers, H, 'H')
    set_bw_distribution(workers, None, None)
    

if __name__ == "__main__":
    main()

