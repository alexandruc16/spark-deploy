import argparse
import itertools
import paramiko
import os
import sys
import time
from subprocess import Popen, PIPE
from time import sleep

pkey = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))

A = [617, 200, 489, 115, 497, 440, 290, 286]
B = [676, 957, 754, 336, 409, 916, 801, 924]
C = [940, 921, 910, 938, 729, 792, 692, 939]
D = [292, 189, 281, 243, 169, 117, 282, 137]
E = [852, 822, 852, 858, 858, 809, 856, 856]
F = [789, 887, 900, 853, 301, 703, 800, 900]
G = [613, 602, 556, 614, 576, 575, 400, 614]
H = [552, 260, 953, 795, 355, 927, 624, 552]


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
            

#def start_monitor_bandwidths(workers):
#    command = 'nohup python /opt/bandwidth-throttler/monitor_bandwidth.py ens3 /opt/bandwidth-throttler/monitor.out /opt/bandwidth-throttler/monitor.in proc 9 1>/dev/null 2>/dev/null &\n'
#    issue_ssh_commands(workers, command)


#def start_bandwidth_throttling_servers(workers):
#    command = 'python /opt/bandwidth-throttler/shape_traffic_server.py --port 2221\n'
#    issue_ssh_commands(workers, command)
    
    
def set_bandwidths(workers, values):
    for i in range(0, len(workers)):
        worker = workers[i]
        command = 'sudo bash /opt/wondershaper/wondershaper -c -a ens3\n'
        
        if values is not None:
            value = 1024 * values[i]
            command += 'sudo bash /opt/wondershaper/wondershaper -a ens3 -u %d -d %d\n' % (value, value)
    
        issue_ssh_commands([worker], command)
    

def run_experiments(workers, values=None, typ=None):
    set_bandwidths(workers, values)
    
    if typ is None:
        typ = "no_limit"
    
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
    
    
def generate_bw_files(workers):
    command = 'nohup python /opt/spark-deploy/scripts/utils/generate_bw_files.py 1>/dev/null 2>/dev/null &\n'
    issue_ssh_commands(workers, command)
    

def main():
    workers = get_workers()
    generate_bw_files(workers)
    run_experiments(workers)
    run_experiments(workers, A, 'A')
    run_experiments(workers, B, 'B')
    run_experiments(workers, C, 'C')
    run_experiments(workers, D, 'D')
    run_experiments(workers, E, 'E')
    run_experiments(workers, F, 'F')
    run_experiments(workers, G, 'G')
    run_experiments(workers, H, 'H')  
    

if __name__ == "__main__":
    main()

