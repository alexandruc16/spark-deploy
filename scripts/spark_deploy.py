#!/usr/bin/python

import argparse
import conf.defaults as defaults
#import oca
import os
import paramiko
import re
import sys
import time
from subprocess import Popen, PIPE
from time import sleep

pkey = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))


def issue_ssh_commands(slaves_list, commands, remote_username, master_ip=None):
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


def get_or_generate_public_key(master_ip, username, verbose):
    try:
        out = Popen(["./master_keys.sh", master_ip, username], stdout = PIPE,
                    stderr = PIPE).communicate()[0].strip("\n")
        if len(out) < 30:
            print(out)
            raise ValueError
        return out
    except (ValueError, OSError) as err:
        print("ERROR: Could not communicate with master")
        sys.exit(0)
    except:
        print("Unknown error Occured")
        raise


def update_master_public_key(master_ssh_key, verbose):
    print('Updating master\'s public key...')
    master_ssh_key = "'Master_SSH_KEY=" + "\"" + master_ssh_key + "\"'"
    
    try:
        if len(master_ssh_key) < 30:
            raise ValueError
        out = Popen(["./update_master_ssh_key.sh",
                     master_ssh_key],
                    stdout = PIPE,
                    stderr = PIPE).communicate()[0]
        print('Master\'s public key updated!')
        return out
        if "ERROR" in out:
            print(out)
            raise ValueError
    except (OSError, ValueError) as err:
        print("Could not update key in user profile")
        if verbose:
            print(err)
        sys.exit(0)
    except:
        print("Unknown error Occured")
        raise


def scp(vm_ip, remote_username, filename, spark_dir, verbose=False):
    try:
        dest = remote_username + "@" + vm_ip + ":" + spark_dir
        print ("copying to " + dest)
        out = Popen(["scp", filename, dest],
                    stdout=PIPE,
                    stderr=PIPE)
        return out
    except (OSError, ValueError) as err:
        print("Could not login...\n Use verbose for detailed error msg")
        if verbose:
            print(err)
        sys.exit(0)
    except:
        print("Unknown error Occured")
        raise


def spawn_slaves(cluster_name, slave_template, num_slaves, api_url=None, api_user=None, api_pass=None):
    slaves_dict = {}
    
    if ((api_url is not None or api_user is not None or api_pass is not None) and
            (api_url is None or api_user is None or api_pass is None)):
        print("If using OpenNebula Cloud API please set the API URL and credentials appropriately in conf/defaults.py")
        sys.exit(1)

    print("Creating Slave Nodes...")
    
    try:
        if api_url is None and api_user is None and api_pass is None:
            for i in range(1, num_slaves + 1):
                # name the slave
                slave_name = "slave" + str(i) + "." + cluster_name
                result = Popen(["onetemplate", "instantiate",
                                slave_template, "--name", slave_name],
                               stdout=PIPE).communicate()[0]

                slave_id = result.strip('VM ID: ').strip('\n')
                vm_info = Popen(["onevm", "show", str(slave_id)],
                                stdout=PIPE).communicate()[0]
                ip_list = re.findall(r'[0-9]+(?:\.[0-9]+){3}', vm_info)
                slaves_dict[slave_name] = ip_list[0]
        #else:
            #vm_ids = []
            #client = oca.Client(api_user + ":" + api_pass, api_url)
            #vm_templ = oca.VmTemplatePool(client)
            
            #for templ in vm_templ:
            #    if templ.id == slave_template:
            #        for i in range(1, num_slaves + 1):
            #            slave_name = "slave" + str(i) + "." + cluster_name
            #            vm_ids.append(templ.instantiate(slave_name))
            
            #vm_pool = oca.VirtualMachinePool(client)
            
            #for vm in vm_pool:
            #    slaves_dict[slave_name] = vm.template.nics[0].ip
    except:
        raise

    print("Slaves Spawned!")
    return slaves_dict


def set_up_hosts_file(master_hostname, master_ip, nodes_dict, remote_username):
    print("Testing ssh and editing hosts file..")

    ssh_commands = ''
    delay = 30
    nodes_dict[master_hostname] = master_ip
    ips = nodes_dict.values()
    ips_count = len(ips)
    nodes_online = []
    
    for node in nodes_dict.iterkeys():
        ssh_commands += "echo '%s %s' | sudo tee -a /etc/hosts\n" % (nodes_dict[node], node)
    
    while ips_count > 0:
        for ip in ips:
            try:
                issue_ssh_commands([ip], ssh_commands, remote_username)
                nodes_online.append(ip)
            except:
                pass
        
        nodes_online_count = len(nodes_online)
        
        if nodes_online_count > 0:
            ips = [ip for ip in ips if ip not in nodes_online]
            ips_count = len(ips)
        
        if ips_count > 0:
            print("%d nodes online, %d to go. Retrying in %d seconds." % (nodes_online_count, ips_count, delay))
            sleep(delay)
    
    print('Hosts file set up!')


def configure_hadoop(hadoop_dir, master_hostname, master_ip, slaves_dict, remote_username):
    print('Configuring Hadoop..')
    conf_dir = os.path.join(hadoop_dir, 'etc/hadoop')
    hdfs_name_dir = os.path.join(hadoop_dir, 'dfs/name')
    hdfs_data_dir = os.path.join(hadoop_dir, 'dfs/name/data')
    hadoop_tmp_dir = os.path.join(hadoop_dir, 'tmp')
    replacements = {
        '{{master_hostname}}': master_hostname, 
        '{{num_workers}}': str(len(slaves_dict.values())),
        '{{dfs_name_dir}}': hdfs_name_dir,
        '{{dfs_data_dir}}': hdfs_data_dir,
        '{{hadoop_tmp_dir}}': hadoop_tmp_dir
    }
    
    ssh_commands = ''

    for r in replacements:
        ssh_commands += 'sudo sed -i \'s?%s?%s?g\' %s\n' % (r, replacements[r], os.path.join(conf_dir, 'core-site.xml'))
        ssh_commands += 'sudo sed -i \'s/%s/%s/g\' %s\n' % (r, replacements[r], os.path.join(conf_dir, 'mapred-site.xml'))
        ssh_commands += 'sudo sed -i \'s?%s?%s?g\' %s\n' % (r, replacements[r], os.path.join(conf_dir, 'hdfs-site.xml'))
        ssh_commands += 'sudo sed -i \'s/%s/%s/g\' %s\n' % (r, replacements[r], os.path.join(conf_dir, 'yarn-site.xml'))
    
    ssh_commands += 'sudo rm -rf %s %s\n' % (os.path.join(conf_dir, 'masters'), os.path.join(conf_dir, 'slaves'))
    ssh_commands += 'sudo echo \'%s\' >> %s\n' % (master_hostname, os.path.join(conf_dir, 'masters'))
    
    for slave_hostname in slaves_dict.keys():
        ssh_commands += 'sudo echo \'%s\' >> %s\n' % (slave_hostname, os.path.join(conf_dir, 'slaves'))
    
    issue_ssh_commands(slaves_dict.values(), ssh_commands, remote_username, master_ip)
    print('Hadoop configured!')


def configure_spark(spark_dir, master_hostname, master_ip, slaves_dict, remote_username):
    print('Configuring Spark..')
    conf_file = os.path.join(spark_dir, 'conf/spark-env.sh')
    replacements = {'{{master_hostname}}': master_hostname}
    ssh_commands = ''

    for r in replacements:
        ssh_commands += 'sudo sed -i \'s/%s/%s/g\' %s\n' % (r, replacements[r], conf_file)
        
    ssh_commands += 'echo \"SPARK_MASTER_HOST=\'%s\'\" | sudo tee -a %s\n' % (master_hostname, conf_file)
    
    for slave_hostname in slaves_dict.keys():
        ssh_commands += 'sudo echo \'%s\' >> %s\n' % (slave_hostname, os.path.join(spark_dir, 'conf/slaves'))
        
    issue_ssh_commands(slaves_dict.values(), ssh_commands, remote_username, master_ip)
    print('Spark configured!')


def generate_bw_files(workers, bw, filename):
    num_workers = len(workers)
    f = open(filename, 'w')
    
    for i in range(0, num_workers):
        for j in range(0, num_workers):
            if i == j:
                continue
            val = "%s:%s:%d" % (workers[i], workers[j], min(bw[i], bw[j]))
            print >> f, val
            
    f.close()


def configure_hibench(hibench_conf_dir, hadoop_dir, spark_dir, master_hostname, master_ip, slaves_dict, remote_username):
    print('Configuring HiBench')
    ssh_commands = ''
        
    replacements = {
        '{{master_hostname}}': master_hostname,
        '{{hadoop_dir}}': hadoop_dir,
        '{{hadoop_conf_dir}}': os.path.join(hadoop_dir, 'etc/hadoop'),
        '{{hadoop_exec}}': os.path.join(hadoop_dir, 'bin/hadoop'),
        '{{spark_dir}}': spark_dir
    }

    for r in replacements:
        ssh_commands += 'sudo sed -i \'s?%s?%s?g\' %s\n' % (r, replacements[r], os.path.join(hibench_conf_dir, 'hadoop.conf'))
        ssh_commands += 'sudo sed -i \'s?%s?%s?g\' %s\n' % (r, replacements[r], os.path.join(hibench_conf_dir, 'spark.conf'))
        
    slaves_dict[master_hostname] = master_ip
    
    issue_ssh_commands(slaves_dict.values(), ssh_commands, remote_username, master_ip)    
    print('HiBench configured!')


def format_namenode(hadoop_dir, master_hostname, remote_username):
    print('Formatting Hadoop namenode..')
    ssh_commands = '%s/bin/hadoop namenode -format\n' % hadoop_dir
    issue_ssh_commands([], ssh_commands, remote_username, master_hostname)
    print('Hadoop namenode formatted!')


def start_hadoop(hadoop_dir, master_hostname, remote_username):
    print('Starting Hadoop..')
    ssh_command = 'bash %s\n' % os.path.join(hadoop_dir, 'sbin/start-all.sh')
    
    issue_ssh_commands([], ssh_command, remote_username, master_hostname)
    print('Hadoop started!')

def start_spark(spark_dir, master_hostname, master_ip, slaves_list, remote_username):
    print('Starting Spark..')
    ssh_command = 'bash %s\n' % os.path.join(spark_dir, 'sbin/start-master.sh')
    
    issue_ssh_commands([], ssh_command, remote_username, master_ip)
    
    ssh_command = 'bash %s spark://%s:7077\n' % (os.path.join(spark_dir, 'sbin/start-slave.sh'), master_hostname)
    
    issue_ssh_commands(slaves_list, ssh_command, remote_username)    
    print('Spark started!')

def check_args(args):
    try:
        args.num_slaves = int(args.num_slaves)
        args.cluster_name = str(args.cluster_name)
        args.master_ip = str(args.master_ip)

        # check that there are any slaves to create
        if args.num_slaves < 1:
            print("There are no slaves to create...")
            sys.exit(0)

        # double check that more than 10 slaves is not a typo
        if args.num_slaves > 10:
            input = raw_input("Are you sure you want to create "
                              + str(args.num_slaves)
                              + " Slaves? (y/n)")
            if input != 'y':
                print("OK, Give it another try")
                sys.exit(0)

        # Check that master has a public ip address
        print("\nTesting ssh into master... ")
        out = Popen(["./test_ssh.sh", args.master_ip], stdout = PIPE,
                    stderr = PIPE).communicate()[0].strip("\n")
        if "ERROR" in out:
            print(out)
            raise ValueError

        print("SSH DONE... ")
        return args

    except (OSError, ValueError) as err:
        print("Please recheck your arguments\n")
        sys.exit(0)


def main():

    parser = argparse.ArgumentParser(description="Create a Spark Cluster on "
                                     "PDC Cloud.", epilog = "Example Usage: "
                                     "./spark_deploy.py -c cluster1 -n 5 -m "
                                     "10.10.10.10")
    parser.add_argument("-r", action="store_true", dest="use_cloud_api",
                        help="Use ONE Cloud API")
    parser.add_argument("--url", metavar="", dest="api_url",
                        action="store", default=defaults.api_url,
                        help="OpenNebula Cloud API URL")
    parser.add_argument("--user", metavar="", dest="api_user",
                        action="store", default=defaults.api_user,
                        help="OpenNebula Cloud API Username")
    parser.add_argument("--pass", metavar="", dest="api_pass",
                        action="store", default=defaults.api_pass,
                        help="OpenNebula Cloud API password")
    parser.add_argument("-c", "--name", metavar="", dest="cluster_name",
                        action="store",
                        default=defaults.cluster_name,
                        help="Name for the cluster.")
    parser.add_argument("-n", "--num-slaves", metavar="", dest="num_slaves",
                        default=defaults.num_slaves,
                        type=int, action="store",
                        help="Number of slave nodes to spawn.")
    parser.add_argument("-m", "--master-ip", metavar="", dest="master_ip",
                        action="store", default=defaults.master_ip,
                        help="Ip address of Master")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", help="verbose output")
    parser.add_argument("-D", "--dryrun", dest="dryrun",
                        action="store_true", help="Dry run")

    args = parser.parse_args()

# Verify arguments
    args = check_args(args)

# If args verified and there were no errors
# set variables
    cluster_name = args.cluster_name
    num_slaves = args.num_slaves
    master_hostname = defaults.master_hostname
    master_ip = args.master_ip
    verbose = args.verbose
    dryrun = args.dryrun
    filename = defaults.filename
    slave_template = defaults.slave_template
    hadoop_dir = defaults.hadoop_dir
    spark_dir = defaults.spark_dir
    hibench_dir = defaults.hibench_dir
    remote_username = defaults.remote_username
    #use_cloud_api = isset(args.use_cloud_api)
    api_url = defaults.api_url
    api_user = defaults.api_user
    api_pass = defaults.api_pass
    

    if dryrun:
        print("\n")
        print("Remote Username: " + str(remote_username))
        print("Cluster Name: " + str(cluster_name))
        print("Master IP: " + str(master_ip))
        print("Number of Slaves: " + str(num_slaves))
        print("Slave template: " + str(slave_template))
        print("Hadoop directory: " + str(hadoop_dir))
        print("Spark directory: " + str(spark_dir))
        print("HiBench directory: " + str(hibench_dir))
        print("\n")
        sys.exit(0)
# Get the Master's public key
    #master_key = get_or_generate_public_key(master_ip, remote_username, verbose)
    #print("\n")
    #print("******** Got the master's SSH key **********")
    #update_status = update_master_public_key(master_key, verbose)
    #if "ERROR" in update_status:
    #    print update_status
    #    sys.exit(0)

    #if verbose:
    #    print(update_status)
    #print("******** Updated master's SSH key **********")
    #print("\n")
# Now create the requested number of slaves
# confirm cluster name
    print("Cluster name will be set to: " + args.cluster_name)
    input = raw_input("To avoid HOSTNAME conflicts, Please verify that "
                      "cluster name is unique... Continue (y/n): ")
    if input == 'n':
        print("Ok, Exit...")
        sys.exit(0)

    slaves_dict = spawn_slaves(cluster_name, slave_template, num_slaves)
    slave_hostnames = []
    print("\n")
    for slave_id, hostname in slaves_dict.items():
        print(hostname + " Created...")
        slave_hostnames.append(str(hostname))
    print("\n")
    
# save slaves hostnames to file
    slave_file = open(filename, "w")
    for host in slave_hostnames:
        slave_file.write(host + "\n")

    slave_hostnames.append(master_ip)

# move slaves file to master's spark conf directory
    scp(master_ip, remote_username, filename, spark_dir, verbose)

    print("\n")
    print("*********** All Slaves Created **************************")
    print("*********** Slaves file Copied to Master ****************")
    print("*********** Waiting for slaves to finish setting up *****")
    print("\n")
    
    # wait until all slaves are available
    set_up_hosts_file(master_hostname, master_ip, slaves_dict, remote_username)
    
    print("*********** Starting up *********************************")
    
    hibench_conf_dir = os.path.join(hibench_dir, 'conf')
    
    configure_hadoop(hadoop_dir, master_hostname, master_ip, slaves_dict, remote_username)
    #format_namenode(hadoop_dir, master_ip, remote_username)
    configure_spark(spark_dir, master_hostname, master_ip, slaves_dict, remote_username)
    configure_hibench(hibench_conf_dir, hadoop_dir, spark_dir, master_hostname, master_ip, slaves_dict, remote_username)
    #start_hadoop(hadoop_dir, master_ip, remote_username)
    #start_spark(spark_dir, master_hostname, master_ip, slaves_dict.values(), remote_username)

if __name__ == "__main__":
    main()

