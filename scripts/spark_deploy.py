#!/usr/bin/python

import argparse
import conf.defaults as defaults
import os
import paramiko
import re
import sys
from subprocess import Popen, PIPE
from time import sleep

pkey = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))


def issue_ssh_commands(slaves_list, commands, remote_username, master_ip=None):
    if not master_ip is None:
        slaves_list.insert(0, master_ip)
            
    for ip in slaves_list:
        try:
            ssh = paramiko.SSHClient()
            ssh.connect(ip, username=remote_username, pkey=pkey)
            channel = ssh.invoke_shell()
            stdin = channel.makefile('wb')
            stdout = channel.makefile('rb')

            stdin.write(commands)
            print(stdout.read())

            stdout.close()
            stdin.close()
            channel.close()
        
        except:
            print("Error occurred while issuing SSH commands to " + ip)
            print(commands)
            raise
            

def test_ssh(ips, remote_username):
    delay = 30
    ips_count = len(ips)
    nodes_online = []
    
    while ips_count > 0:
        for ip in ips:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            try:
                ssh.connect(ip, username=remote_username, pkey=pkey)
                nodes_online.append(ip)
            except:
                print("Failed to connect to " + ip)
        
        nodes_online_count = len(nodes_online)
        
        if nodes_online_count > 0:
            ips = [ip for ip in ips if ip not in nodes_online]
            ips_count = len(ips)
        
        if ips_count > 0:
            print("%d nodes online, %d to go. Retrying in %d seconds." % (nodes_online_count, ips_count, delay))
            sleep(delay)  


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


def spawn_slaves(cluster_name, slave_template, num_slaves):
    slaves_dict = {}

    print("Creating Slave Nodes...")
    try:
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

            # slaves_dict[slave_id] = ip_list[0]
            slaves_dict[slave_id] = ip_list[0]
    except:
        raise

    print("Slaves Spawned!")
    return slaves_dict


def set_up_hosts_file(master_ip, slaves_dict, remote_username):
    print("Editing hosts file..")

    ssh_commands = ''

    try:
        s = "%s\tmaster" % master_ip
        ssh_commands += "cat %s >> /etc/hosts\n" % s
        for slave in slaves_dict.iterkeys():
            s = "%s\t%s" % (slaves_dict[slave], slave)
            ssh_commands += "cat %s >> /etc/hosts\n" % s
        issue_ssh_commands(slaves_dict.values(), remote_username, master_ip)
        print('Hosts file set up!')
    except:
        raise


def configure_hadoop(hadoop_dir, master_hostname, slaves_list, remote_username):
    print('Configuring Hadoop..')
    replacements = {'{{master_hostname}}': master_hostname, '{{num_workers}}': str(len(slaves_list))}
    conf_dir = os.path.join(hadoop_dir, '/etc/hadoop')
    
    ssh_commands = ''
    ssh_commands += 'cd %s\n' % conf_dir

    for r in replacements:
        ssh_commands += 'sed -i \'s/%s/%s/g\' core-site.xml\n' % (r, replacements[r])
        ssh_commands += 'sed -i \'s/%s/%s/g\' mapred-site.xml\n' % (r, replacements[r])
        ssh_commands += 'sed -i \'s/%s/%s/g\' hdfs-site.xml\n' % (r, replacements[r])

    issue_ssh_commands(slaves_list, ssh_commands, remote_username, master_hostname)
    print('Hadoop configured!')


def configure_spark(spark_dir, master_hostname, slaves_dict, remote_username):
    print('Configuring Spark..')
    conf_file = os.path.join(spark_dir, '/conf/spark-env.sh')
    replacements = {'{{master_hostname}}': master_hostname}
    ssh_commands = 'cd %s\n' % spark_dir
    ssh_commands += 'touch slaves\n'

    for ip in slaves_dict.values():
        ssh_commands += 'cat %s >> slaves\n' % ip

    for r in replacements:
        ssh_commands += 'sed -i \'s/%s/%s/g\' %d\n' % (r, replacements[r], conf_file)

    issue_ssh_commands(slaves_dict.values(), ssh_commands, remote_username, master_hostname)
    print('Spark configured!')


def configure_hibench(hibench_dir, master_hostname, slaves_list, remote_username):
    print('Configuring HiBench')
    ssh_commands = ''
    replacements = {'{{master_hostname}}': master_hostname}
    f = os.path.join(hibench_dir, 'hadoop.conf')

    for r in replacements:
        ssh_commands += 'sed -i \'s/%s/%s/g\' %d\n' % (r, replacements[r], f)
    issue_ssh_commands(slaves_list, ssh_commands, remote_username, master_hostname)
    print('HiBench configured!')


def format_namenode(hadoop_dir, master_hostname, slaves_list, remote_username):
    print('Formatting Hadoop namenode..')
    ssh_commands = '%s/bin/hadoop namenode -format\n' % hadoop_dir
    issue_ssh_commands(slaves_list, ssh_commands, remote_username, master_hostname)
    print('Hadoop namenode formatted!')


def start_hadoop(hadoop_dir, master_hostname, remote_username):
    print('Starting Hadoop..')
    ssh_commands = '%s/sbin/start-dfs.sh\n' % hadoop_dir
    ssh_commands += '%s/sbin/start-yarn.sh\n' % hadoop_dir
    
    issue_ssh_commands([], ssh_commands, remote_username, master_hostname)
    print('Hadoop started!')

def start_spark(spark_dir):
    print('Starting Spark..')
    ssh_commands = '%s/sbin/start-all.sh\n' % spark_dir
    
    issue_ssh_commands([], ssh_commands, remote_username, master_hostname)
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
    test_ssh(slave_hostnames, remote_username)
    
    print("*********** Starting up *********************************")
    
    #configure_hadoop(hadoop_dir, master_hostname, slave_hostnames, remote_username)
    #configure_spark(spark_dir, master_hostname, slaves_dict, remote_username)
    #configure_hibench(hibench_dir, master_hostname, slave_hostnames, remote_username)

if __name__ == "__main__":
    main()

