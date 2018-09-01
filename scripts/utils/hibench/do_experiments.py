import argparse
import itertools
import json
import paramiko
import os
import sys
import time
import urllib
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


def format_namenode():
    print("Formatting namenode")
    try:
        cmd_res = Popen("echo Y | hdfs namenode -format", shell=True, stdout=PIPE).communicate()[0]
    except Exception as e:
        print(e)


def clear_hdfs(workers):
    command = 'rm -rf /usr/local/hadoop/dfs/*'
    command += 'rm -rf /usr/local/hadoop/tmp/*'
    command += 'mkdir -p /usr/local/hadoop/dfs/name/data'

    for i in range(0, len(workers)):
        worker = workers[i]
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


def print_failed_log(experiment, log_type):
    log_location = os.path.join('/opt/hibench/report', experiment, log_type, 'bench.log')

    if os.path.isfile(log_location):
        with open(log_location, 'r') as f:
            print f.read()


def clear_spark_history():
    try:
        cmd_res = Popen(["hdfs", "dfs", "-rm", "/spark-events/*"], stdout=PIPE, stderr=PIPE).communicate()[0]
    except Exception as e:
        print(e)


def get_last_spark_log(experiment, bw_conf):
    reports_dir = "/opt/spark-deploy/scripts/utils/task-reports/"

    if not os.path.exists(reports_dir):
        os.path.makedirs(reports_dir)

    hist_server = "http://master.aca540:18080/api/v1"
    last_app_url = "/applications?limit=1"
    resp = urllib.urlopen(hist_server + last_app_url)
    data = json.loads(resp.read())
    app_id = data[0]['id']
    stages_url = "/applications/%s/stages" % app_id
    resp = urllib.urlopen(hist_server + stages_url)
    data = json.loads(resp.read())

    for stage in data:
        stage = str(stage["stageId"])
        attempt = str(stage["attemptId"])
        summary_url = "/applications/%s/stages/%s/%s/taskSummary?quantiles=0.01,0.25,0.5,0.75,0.99" % (app_id, stage, attempt)
        resp = urllib.urlopen(hist_server + summary_url)
        data = json.loads(resp.read())
        filename = "%s_%s_%s_%s_summary.json" % (experiment, bw_conf, stage, attempt)
        filepath = os.path.join(reports_dir, filename)

        with open(filepath, "w+") as f:
            json.dump(data, f)


def prepare_hibench_experiment(experiment, exp_folder, workers):
    print("Preparing experiment: " + experiment)
    set_bw_distribution(workers, None, None, None)
    print("Clearing HDFS")
    cmd_res = Popen(["hdfs", "dfs", "-rm", "-r", "-skipTrash", "/tmp", "/HiBench"], stdout=PIPE, stderr=PIPE).communicate()[0]
    stop_cluster()
    #clear_hdfs(workers)
    #format_namenode()
    start_cluster()

    print("Generating data for experiment: " + experiment)
    prepare_location = os.path.join(exp_folder, 'prepare/prepare.sh')

    try:
        prepare_process = Popen(["bash", prepare_location], stdout=PIPE, stderr=PIPE)
        cmd_res = prepare_process.communicate()[0]

        if prepare_process.returncode != 0:
            print_failed_log(experiment, 'prepare')
            print("Prepare failed for " + experiment)
            return 1
    except Exception as e:
        print(e)

    return 0

    
def run_hibench_experiment(experiment, exp_folder, times):
    run_location = os.path.join(exp_folder, 'spark/run.sh')

    for i in range(0, times):
        start_spark()
        print(experiment + ": Running " + experiment + " #" + str(i + 1))
        run_process = Popen(["bash", run_location], stdout=PIPE, stderr=PIPE)
        cmd_res = run_process.communicate()[0]

        if run_process.returncode != 0:
            print_failed_log(experiment, 'spark')
            print("Experiment " + experiment + "failed at run " + str(i))
            return


def do_hibench_experiment(experiment, exp_folder, workers, iterations=10):
    set_bw_distribution(workers, None, None, None)
    if prepare_hibench_experiment(experiment, exp_folder, workers) > 0:
        return

    set_bw_distribution(workers, experiment, 'no_limit', NO_LIMIT_CONFIGURATION)
    run_hibench_experiment(experiment, exp_folder, iterations)
    #get_last_spark_log(experiment, "nolimit")
    #clear_spark_history()

    bandwidth_configs = sorted(BANDWIDTH_CONFIGURATIONS.keys())
    
    for key in bandwidth_configs:
        set_bw_distribution(workers, experiment, key, BANDWIDTH_CONFIGURATIONS[key])
        run_hibench_experiment(experiment, exp_folder, iterations)
        #get_last_spark_log(experiment, key)
        #clear_spark_history()
    
    fnam = '/opt/hibench/report/%s.report' % experiment    
    cmd_res = Popen(["mv", '/opt/hibench/report/hibench.report', fnam], stdout=PIPE, stderr=PIPE).communicate()[0]
    
    
def do_hibench_experiments(workers):
    format_namenode()
    do_hibench_experiment('sort', '/opt/hibench/bin/workloads/micro/sort', workers)
    do_hibench_experiment('terasort', '/opt/hibench/bin/workloads/micro/terasort', workers)
    do_hibench_experiment('wordcount', '/opt/hibench/bin/workloads/micro/wordcount', workers)
    do_hibench_experiment('kmeans', '/opt/hibench/bin/workloads/ml/kmeans', workers)
    do_hibench_experiment('bayes', '/opt/hibench/bin/workloads/ml/bayes', workers)
    do_hibench_experiment('pagerank', '/opt/hibench/bin/workloads/websearch/pagerank', workers)
    
    print("Finished running HiBench experiments")
    set_bw_distribution(workers, None, None, None)  # clear bw limits when done
    print("Cleared bandwidth limits and stopped monitors. All done!")
    

def main():
    workers = get_workers()
    do_hibench_experiments(workers)
    print("Program ended")
    

if __name__ == "__main__":
    try:
        main()
    except:
        print("Stopping hadoop")
        cmd_res = Popen(["bash", '/usr/local/hadoop/sbin/stop-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]
        
        print("Stopping spark")
        cmd_res = Popen(["bash", '/usr/local/spark/sbin/stop-all.sh'], stdout=PIPE, stderr=PIPE).communicate()[0]

