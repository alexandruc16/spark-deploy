import argparse
import itertools
import paramiko
import os
import sys
import time
from subprocess import Popen, PIPE
from time import sleep

pkey = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))

A = [440, 497, 489, 617, 290, 286, 359, 356, 244, 255, 218, 226, 63, 135, 116, 115]
B = [916, 957, 924, 801, 754, 676, 758, 654, 584, 635, 557, 623, 443, 501, 409, 336]
C = [938, 940, 939, 940, 921, 910, 930, 904, 855, 846, 894, 870, 680, 692, 729, 792]
D = [243, 282, 281, 292, 189, 169, 184, 190, 166, 168, 167, 138, 117, 130, 137, 117]
E = [868, 868, 856, 856, 852, 853, 852, 853, 854, 848, 844, 830, 797, 791, 809, 822]
F = [900, 846, 887, 853, 791, 800, 789, 814, 741, 736, 770, 745, 690, 490, 703, 301]
G = [602, 604, 614, 613, 576, 584, 556, 593, 535, 538, 533, 534, 417, 480, 400, 487]
H = [953, 927, 679, 795, 552, 624, 531, 526, 436, 474, 466, 461, 332, 251, 355, 260]


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
    
    
def set_bandwidth(workers, filename):
    command = 'nohup python /opt/bandwidth-throttler/shape_traffic_client.py --port 2221 --set-file %s 1>/dev/null 2>/dev/null &\n' % filename 
    issue_ssh_commands(workers, command)
    print(command)
    

def run_experiments(workers, typ=None):
    if typ is not None:
	filename = "/opt/spark-deploy/scripts/utils/%s.txt" % typ 
        set_bandwidth(workers, filename)
    else:
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
    run_experiments(workers, 'A')
    run_experiments(workers, 'B')
    run_experiments(workers, 'C')
    run_experiments(workers, 'D')
    run_experiments(workers, 'E')
    run_experiments(workers, 'F')
    run_experiments(workers, 'G')
    run_experiments(workers, 'H')  
    

if __name__ == "__main__":
    main()

