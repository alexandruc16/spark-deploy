#!/usr/bin/python

from subprocess import Popen, PIPE
import argparse
import os
import paramiko
import re
import sys
import conf.defaults as defaults


pkey = paramiko.RSAKey.from_private_key(os.path.expanduser("~/.ssh/id_rsa"))


def ReplaceInFile(file, d):
    f = open(file, 'r')
    contents = f.read()
    f.close()

    for s in d:
        contents.replace(s, d[s])

    f = open(file, 'w')
    f.write(contents)


def IssueSSHCommands(slaves_list, commands, remote_username):
    try:
        for ip in slaves_list:
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
        raise


def GetOrGeneratePubKey(master_ip, username, verbose):
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


def UpdateMasterPubKey(master_ssh_key, verbose):

    master_ssh_key = "'Master_SSH_KEY=" + "\"" + master_ssh_key + "\"'"
    try:
        if len(master_ssh_key) < 30:
            raise ValueError
        out = Popen(["./update_master_ssh_key.sh",
                     master_ssh_key],
                    stdout = PIPE,
                    stderr = PIPE).communicate()[0]
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


def Scp(vm_ip, remote_username, filename, spark_dir, verbose=False):

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


def SpawnSlaves(cluster_name, slave_template, num_slaves):

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
        print(slaves_dict)

    except:
        raise

    print("Slaves Spawned...")
    return slaves_dict


def SetupHostsFile(master_ip, slaves_dict, remote_username):
    print("Editing hosts file")

    ssh_commands = ''

    try:
        s = "%s\tmaster" % master_ip
        ssh_commands += "cat %s >> /etc/hosts\n" % s
        for slave in slaves_dict.iterkeys():
            s = "%s\t%s" % (slaves_dict[slave], slave)
            ssh_commands += "cat %s >> /etc/hosts\n" % s
        IssueSSHCommands(slaves_dict.values(), remote_username)
    except:
        raise


def ConfigureHadoop(hadoop_dir, master_hostname, slaves_list, remote_username):
    replacements = {'{{master_hostname}}': master_hostname, '{{num_workers}}': str(len(slaves_list))}

    ssh_commands = ''
    ssh_commands += 'cd %s\n' % hadoop_dir

    for r in replacements:
        ssh_commands += 'sed -i \'s/%s/%s/g\' core-site.xml\n' % (r, replacements[r])
        ssh_commands += 'sed -i \'s/%s/%s/g\' mapred-site.xml\n' % (r, replacements[r])
        ssh_commands += 'sed -i \'s/%s/%s/g\' hdfs-site.xml\n' % (r, replacements[r])

    IssueSSHCommands(slaves_list, ssh_commands, remote_username)


def configure_spark(spark_dir, master_hostname, slaves_dict, remote_username):
    conf_file = os.path.join(spark_dir, 'spark-env.sh')
    replacements = {'{{master_hostname}}': master_hostname}
    ssh_commands = 'cd %s\n' % spark_dir
    ssh_commands += 'touch slaves\n'

    for ip in slaves_dict.values():
        ssh_commands += 'cat %s >> slaves\n' % ip

    for r in replacements:
        ssh_commands += 'sed -i \'s/%s/%s/g\' %d\n' % (r, replacements[r], conf_file)

    IssueSSHCommands(slaves_dict.values(), ssh_commands, remote_username)


def configure_hibench(hibench_dir, master_hostname, slaves_list, remote_username):
    ssh_commands = ''
    replacements = {'{{master_hostname}}': master_hostname}
    f = os.path.join(hibench_dir, '/conf/hadoop.conf')

    for r in replacements:
        ssh_commands += 'sed -i \'s/%s/%s/g\' %d\n' % (r, replacements[r], f)
    IssueSSHCommands(slaves_list, ssh_commands, remote_username)


def format_namenode(hadoop_dir):
    script = os.path.join(hadoop_dir, '/bin/hadoop')
    c = Popen([script, 'namenode', '-format'], stdout=PIPE).communicate()[0]


def start_hadoop(hadoop_dir):
    start_dfs = os.path.join(hadoop_dir, '/sbin/start-dfs.sh')
    start_yarn = os.path.join(hadoop_dir, '/sbin/start-yarn.sh')
    c = Popen([start_dfs], stdout=PIPE).communicate()[0]
    c = Popen([start_yarn], stdout=PIPE).communicate()[0]


def start_spark(spark_dir):
    start = os.path.join(spark_dir, '/sbin/start-all.sh')
    c = Popen([start], stdout=PIPE).communicate()[0]


def CheckArgs(args):
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
    args = CheckArgs(args)

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
    remote_username = defaults.remote_username

    if dryrun:
        print("\n")
        print("Remote Username: " + str(remote_username))
        print("Cluster Name: " + str(cluster_name))
        print("Master IP: " + str(master_ip))
        print("Number of Slaves: " + str(num_slaves))
        print("Slave template: " + str(slave_template))
        print("Hadoop Dir on Master: " + str(hadoop_dir))
        print("Spark Dir on Master: " + str(spark_dir))
        print("\n")
        sys.exit(0)
# Get the Master's public key
    master_key = GetOrGeneratePubKey(master_ip, remote_username, verbose)
    print("\n")
    print("******** Got the master's SSH key **********")
    update_status = UpdateMasterPubKey(master_key, verbose)
    if "ERROR" in update_status:
        print update_status
        sys.exit(0)

    if verbose:
        print(update_status)
    print("******** Updated master's SSH key **********")
    print("\n")
# Now create the requested number of slaves
# confirm cluster name
    print("Cluster name will be set to: " + args.cluster_name)
    input = raw_input("To avoid HOSTNAME conflicts, Please verify that "
                      "cluster name is unique... Continue (y/n): ")
    if input == 'n':
        print("Ok, Exit...")
        sys.exit(0)

    slaves_dict = SpawnSlaves(cluster_name, slave_template, num_slaves)
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

    slave_hostnames.append(master_hostname)

# move slaves file to master's spark conf directory
    Scp(master_ip, remote_username, filename, spark_dir, verbose)

    print("\n")
    print("*********** All Slaves Created *************")
    print("*********** Slaves file Copied to Master *************")
    print("*********** Check that all slaves are RUNNING *************")
    print("*********** Login to Master and run start_all.sh *************")
    print("\n")


if __name__ == "__main__":
    main()
