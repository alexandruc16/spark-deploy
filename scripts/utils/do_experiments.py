import argparse
import itertools
import paramiko
import os
import sys
import time
from subprocess import Popen, PIPE
from time import sleep

pkey = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))

A = [497, 286, 359, 226, 135, 489, 244, 116, 218, 290, 617, 356, 63, 115, 255, 440]
B = [754, 501, 801, 924, 623, 557, 916, 635, 957, 336, 584, 409, 443, 654, 676, 758]
C = [692, 894, 729, 921, 870, 855, 940, 910, 904, 939, 938, 930, 846, 792, 940, 680]
D = [243, 117, 292, 117, 190, 169, 189, 184, 167, 138, 282, 168, 166, 130, 281, 137]
E = [844, 809, 852, 791, 797, 834, 822, 856, 853, 852, 848, 830, 858, 858, 856, 853]
F = [690, 846, 853, 814, 745, 741, 736, 791, 887, 800, 301, 789, 490, 703, 900, 770]
G = [604, 556, 576, 417, 534, 533, 400, 613, 535, 487, 538, 584, 614, 602, 593, 480]
H = [436, 332, 531, 795, 526, 461, 624, 260, 251, 355, 953, 466, 474, 552, 927, 679]

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
    #generate_bw_files(workers)
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

