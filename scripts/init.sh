#!/bin/bash
NC='\033[0m' # no color
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NET_INTERFACE_NAME=$(ls -1 /sys/class/net | head -1)

# Part of the code below is intellectual property of O.A. Danner (o.a.danner@student.vu.nl)
# Function that launches the given command and retries it until it fails.
function launch_and_retry {
	"$@"
	local status=$?
	while [ $status -ne 0 ]; do
		sleep 1
		echo "Could not launch $@, retrying" >> /home/$USERNAME/contextualization.log
		"$@"
		local status=$?
	done
}

# Disable SSH while setting up
sudo service ssh stop &>> /var/log/context.log

if [ ! -z "$SSH_PUBLIC_KEY" ]; then
	useradd -s /bin/bash -m $USERNAME
	echo "$USERNAME:1234" | chpasswd
	sed -i "s/.*PasswordAuthentication.*/PasswordAuthentication yes/" /etc/ssh/sshd_config
	sed -i "s/.*MaxStartups.*/Maxstartups 10000/" /etc/ssh/sshd_config
	sed -i "s/.*StrictHostKeyChecking.*/StrictHostKeyChecking no/" /etc/ssh/ssh_config
	mkdir -p /home/$USERNAME/.ssh/
	cat "$SSH_PUBLIC_KEY" >> /home/$USERNAME/.ssh/authorized_keys
	chown -R $USERNAME:$USERNAME /home/$USERNAME/.ssh
	# chmod -R 600 /home/$USERNAME/.ssh/authorized_keys
	chmod 600 /home/$USERNAME/.ssh/authorized_keys

	# add sudo to look around on system:
	echo "$USERNAME ALL=(ALL) NOPASSWD: /bin/bash *" >>/etc/sudoers
	# check:
	cp /etc/sudoers /etc/sudoers.copy
	chmod 644 /etc/sudoers.copy
	mkdir -p /root/.ssh
	cat "$SSH_PUBLIC_KEY" >> /root/.ssh/authorized_keys
	chmod -R 600 /root/.ssh/
	chmod 600 /root/.ssh/authorized_keys
	chmod 700 /root/.ssh
fi

touch /home/$USERNAME/contextualization.log
echo "Hostname is : $HOSTNAME" >> /home/$USERNAME/contextualization.log
echo "Netmask is : $NETMASK" >> /home/$USERNAME/contextualization.log
echo "Gateway is : $GATEWAY" >> /home/$USERNAME/contextualization.log
echo "DNS is : $DNS" >> /home/$USERNAME/contextualization.log

# Not sure if setting the DNS this way does anything..
if [ -n "$DNS" ]; then
        echo "Setting DNS server to $DNS" >>/var/log/context.log
        echo "nameserver $DNS" >/etc/resolv.conf
fi

# Set correct IP address
# Afterwards, we can set the correct netmask.
if [ -n "$NETMASK" ]; then
	ifconfig $NET_INTERFACE_NAME $IP_PUBLIC
	echo "SETTING NETMASK: $NETMASK\n" >> /var/log/netmask.log
	ifconfig $NET_INTERFACE_NAME netmask $NETMASK &>> /var/log/netmask.log
else
	echo "NETMASK DOES NOT EXIST\n" >> /var/log/netmask.log
fi

# Add the gateway.
if [ -n "$GATEWAY" ]; then
	echo "SETTING GATEWAY: $GATEWAY\n" >> /var/log/netmask.log
	route add default gw $GATEWAY
else
	echo "GATEWAY DOES NOT EXIST\n" >> /var/log/netmask.log
fi

sudo apt-get install -f 2>> /home/$USERNAME/contextualization.log
sudo apt-get update 2>> /home/$USERNAME/contextualization.log
sudo apt-get -y upgrade 2>> /home/$USERNAME/contextualization.log

## dev tools
echo -e "${YELLOW}Installing Development Tools${NC}"
INSTALL_PKGS="gcc make flex bison byacc git maven sbt"
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 2>> /home/$USERNAME/contextualization.log
sudo apt-get update 2>> /home/$USERNAME/contextualization.log
for pkg in $INSTALL_PKGS; do
    sudo apt-get -y install $pkg 2>> /home/$USERNAME/contextualization.log
done
echo -e "${GREEN}*********** dev tools Done ************${NC}"

## Python
echo -e "${YELLOW}Installing Python and required libraries${NC}"
sudo apt-get -y install python2.7 python-pip 2>> /home/$USERNAME/contextualization.log
pip2 install paramiko pyzmq psutil 2>> /home/$USERNAME/contextualization.log
echo -e "${GREEN}*********** Python Done ************${NC}"


DOWNLOAD_DIR=~/Downloads
ENVIRONMENT=/etc/environment
source $ENVIRONMENT

## Benchmark scripts
echo -e "${YELLOW}Preparing benchmark scripts${NC}"
cd /opt
rm -rf /opt/spark-deploy
sudo git clone https://github.com/alexandruc16/spark-deploy.git
cd spark-deploy/config-files
CONFIG_DIR="$(pwd)"
echo -e "${GREEN}*********** Benchmarking Scripts Done ************${NC}"

## JAVA
javac -help >/dev/null 2>&1
if [ $? == 0 ]; then
    echo -e "${GREEN}Java was found${NC}"
else
    echo -e "${YELLOW}Installing Oracle JDK v1.8${NC}"
    wget_output=$(wget --no-check-certificate -c --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u171-b11/512cd62ec5174c3487ac17c61aaa89e8/jdk-8u171-linux-x64.tar.gz -P $DOWNLOAD_DIR)
    if [ $? -ne 0 ]; then
        echo "Failed to download Oracle JDK. Please install manually." >> /var/log/context.log
    else
        tar -xzf $DOWNLOAD_DIR/jdk-8u171-linux-x64.tar.gz -C $DOWNLOAD_DIR
        mv $DOWNLOAD_DIR/jdk1.8.0_171 /usr/lib
        if [ -z "$JAVA_HOME" ]; then
            echo "export JAVA_HOME=/usr/lib/jdk1.8.0_171" >> $ENVIRONMENT
            echo "export PATH=$PATH:/usr/lib/jdk1.8.0_171/bin" >> $ENVIRONMENT
            source $ENVIRONMENT
        fi
        echo -e "${GREEN}***** JAVA DONE! *****${NC}"
    fi
fi

# Initialize directories
PROJ_DIR=/usr/local
HADOOP_DIR=$PROJ_DIR/hadoop
SCALA_DIR=$PROJ_DIR/scala
SPARK_DIR=$PROJ_DIR/spark
HIBENCH_DIR=/opt/hibench
SPARK_PERF_DIR=/opt/spark-perf
BIGBENCH_DIR=/opt/big-bench
HIVE_DIR=$PROJ_DIR/hive
GRAPHALYTICS_CORE_DIR=/opt/graphalytics

if [ ! -d $DOWNLOAD_DIR ]; then
    mkdir $DOWNLOAD_DIR
fi

if [ ! -d $PROJ_DIR ]; then
    mkdir $PROJ_DIR
fi


## HADOOP
CMD_OUTPUT=$(command -v hadoop)
if [ -z "$CMD_OUTPUT" ]; then
    echo -e "${YELLOW}Installing Hadoop v2.7.5${NC}"
    wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.5/hadoop-2.7.5.tar.gz -P $DOWNLOAD_DIR
    tar -xzf $DOWNLOAD_DIR/hadoop-2.7.5.tar.gz -C $DOWNLOAD_DIR
    if [ ! -d $HADOOP_DIR ]; then
        mkdir $HADOOP_DIR
    fi
    mv $DOWNLOAD_DIR/hadoop-2.7.5/* $HADOOP_DIR
    if [ -z "$HADOOP_PREFIX" ]; then
        echo "export HADOOP_PREFIX=$HADOOP_DIR" >> $ENVIRONMENT
        echo "export HADOOP_MAPRED_HOME=$HADOOP_DIR" >> $ENVIRONMENT
        echo "export HADOOP_COMMON_HOME=$HADOOP_DIR" >> $ENVIRONMENT
        echo "export HADOOP_HDFS_HOME=$HADOOP_DIR" >> $ENVIRONMENT
        echo "export YARN_HOME=$HADOOP_DIR" >> $ENVIRONMENT
        echo "export PATH=$PATH:$HADOOP_DIR/bin:$HADOOP_DIR/sbin" >> $ENVIRONMENT
        source $ENVIRONMENT
    fi
    echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_DIR/etc/hadoop/hadoop-env.sh
    chmod -R 777 $HADOOP_DIR
    echo -e "${GREEN}***** HADOOP DONE! *****${NC}"
else
    echo -e "${GREEN}Hadoop was found${NC}"
    HADOOP_DIR=$CMD_OUTPUT
fi
yes | cp -a $CONFIG_DIR/hadoop/* $HADOOP_COMMON_HOME/etc/hadoop/
sudo mkdir -p $HADOOP_DIR/dfs/
sudo mkdir -p $HADOOP_DIR/dfs/name
sudo mkdir -p $HADOOP_DIR/dfs/name/data


## SCALA
scalac -help >/dev/null 2>&1
if [ $? == 0 ]; then
    echo -e "${GREEN}Scala was found${NC}"
else
    echo -e "${YELLOW}Installing Scala v2.11.12${NC}"
    wget https://downloads.lightbend.com/scala/2.11.12/scala-2.11.12.tgz -P $DOWNLOAD_DIR
    tar -xzf $DOWNLOAD_DIR/scala-2.11.12.tgz -C $DOWNLOAD_DIR
    if [ ! -d $SCALA_DIR ]; then
        mkdir $SCALA_DIR
    fi
    mv $DOWNLOAD_DIR/scala-2.11.12/* $SCALA_DIR
    chmod -R 777 $SCALA_DIR
    if [ -z "$SCALA_HOME" ]; then
        echo "export SCALA_HOME=$SCALA_DIR" >> $ENVIRONMENT
        echo "export PATH=$PATH:$SCALA_DIR/bin" >> $ENVIRONMENT
        source $ENVIRONMENT
    fi
    echo -e "${GREEN}***** SCALA DONE! *****${NC}"
fi


## SPARK
CMD_OUTPUT=$(command -v spark-submit)
if [ -z "$CMD_OUTPUT" ]; then
    echo -e "${YELLOW}Installing Spark v2.2.1${NC}"
    wget http://apache.mirror.triple-it.nl/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz -P $DOWNLOAD_DIR
    tar -xzf $DOWNLOAD_DIR/spark-2.2.1-bin-hadoop2.7.tgz -C $DOWNLOAD_DIR/
    if [ ! -d $SPARK_DIR ]; then
        mkdir $SPARK_DIR
    fi
    mv $DOWNLOAD_DIR/spark-2.2.1-bin-hadoop2.7/* $SPARK_DIR
    chmod -R 777 $SPARK_DIR
    if [ -z "$SPARK_HOME" ]; then
        echo "export SPARK_HOME=$SPARK_DIR" >> $ENVIRONMENT
        echo "export PATH=$PATH:$SPARK_DIR/bin:$SPARK_DIR/sbin" >> $ENVIRONMENT
        source $ENVIRONMENT
    fi
    source $ENVIRONMENT
    echo -e "${GREEN}*********** Spark Done ************${NC}"
else
    echo -e "${GREEN}Spark was found${NC}"
    SPARK_DIR=$SPARK_HOME
fi
yes | cp -a $CONFIG_DIR/spark/spark-env.sh $SPARK_DIR/conf/spark-env.sh
echo "HADOOP_CONF_DIR=$HADOOP_DIR/etc/hadoop" >> $SPARK_DIR/conf/spark-env.sh


## HiBench
echo -e "${YELLOW}Installing HiBench${NC}"
rm -rf $HIBENCH_DIR
mkdir $HIBENCH_DIR
wget https://github.com/intel-hadoop/HiBench/archive/HiBench-7.0.tar.gz -P $DOWNLOAD_DIR
tar -xzf $DOWNLOAD_DIR/HiBench-7.0.tar.gz -C $DOWNLOAD_DIR/
mv $DOWNLOAD_DIR/HiBench-HiBench-7.0/* $HIBENCH_DIR
cd $HIBENCH_DIR
mvn -Psparkbench -Dspark=2.2 -Dscala=2.11 clean package
yes | cp -a $CONFIG_DIR/hibench/* $HIBENCH_DIR/conf/
touch $HIBENCH_DIR/conf/hadoop.conf
echo "hibench.hadoop.home   $HADOOP_DIR" >> $HIBENCH_DIR/conf/hadoop.conf
echo "hibench.hadoop.executable   $HADOOP_DIR/bin/hadoop" >> $HIBENCH_DIR/conf/hadoop.conf
echo "hibench.hadoop.configure.dir   $HADOOP_DIR/etc/hadoop" >> $HIBENCH_DIR/conf/hadoop.conf
echo "hibench.hadoop.release   apache" >> $HIBENCH_DIR/conf/hadoop.conf
echo "hibench.hdfs.master   hdfs://{{master_hostname}}:9000" >> $HIBENCH_DIR/conf/hadoop.conf
echo "hibench.spark.home   $HADOOP_DIR" >> $HIBENCH_DIR/conf/spark.conf
echo "hibench.spark.master   spark://{{master_hostname}}:7077" >> $HIBENCH_DIR/conf/spark.conf
echo "hibench.spark.version   spark2.2" >> $HIBENCH_DIR/conf/spark.conf
echo -e "${GREEN}*********** HiBench Done ************${NC}"

cd /opt

## Bandwidth throttler
echo -e "${YELLOW}Installing bandwidth-throttler${NC}"
rm -rf /opt/bandwidth-throttler
sudo git clone https://github.com/alexandruc16/bandwidth-throttler.git
rm -rf /usr/bin/shape_traffic
mkdir /usr/bin/shape_traffic
cp /opt/bandwidth-throttler/shape_traffic.sh /usr/bin/shape_traffic
echo -e "${GREEN}*********** bandwidth-throttler Done ************${NC}"

## TPC-DS
echo -e "${YELLOW}Preparing TPC-DS${NC}"
rm -rf /opt/spark-tpc-ds-performance-test
sudo git clone https://github.com/IBM/spark-tpc-ds-performance-test.git
sed -i 's@export SPARK_HOME=@export SPARK_HOME='"$SPARK_DIR"'@g' /opt/spark-tpc-ds-performance-test/bin/tpcdsenv.sh
echo -e "${GREEN}*********** TPC-DS Done ************${NC}"

echo "Finished installing packages" >> /var/log/context.log

## BigBench (TPCx-BB)
export INSTALL_DIR="/opt/big-bench"
cd $INSTALL_DIR
sudo git clone https://github.com/intel-hadoop/Big-Data-Benchmark-for-Big-Bench.git

## Hive
if [ -z "$HIVE_HOME" ]; then
    echo -e "${YELLOW}Installing Hive v2.3.3${NC}"
    wget https://archive.apache.org/dist/hive/hive-2.3.3/apache-hive-2.3.3-bin.tar.gz -P $DOWNLOAD_DIR
    tar -xzf $DOWNLOAD_DIR/apache-hive-2.3.3-bin.tar.gz -C $DOWNLOAD_DIR
    if [ ! -d $HIVE_DIR ]; then
        mkdir $HIVE_DIR
    fi
    mv $DOWNLOAD_DIR/apache-hive-2.3.3-bin/* $HADOOP_DIR
    echo "export HIVE_HOME=$HIVE_DIR" >> $ENVIRONMENT
    source $ENVIRONMENT
    echo "export PATH=$PATH:$HIVE_DIR" >> $ENVIRONMENT
    $HADOOP_DIR/bin/hdfs dfs -mkdir -p /$USERNAME/hive/warehouse
    $HADOOP_DIR/bin/hdfs dfs -mkdir /tmp
    $HADOOP_DIR/bin/hdfs dfs -chmod g+w /$USERNAME/hive/warehouse
    $HADOOP_DIR/bin/hdfs dfs -chmod g+w /tmp
    echo "HADOOP_HOME=$HADOOP_DIR" >> $HIVE_DIR/conf/hive-env.sh
    echo "HIVE_CONF_DIR=$HIVE_DIR/conf" >> $HIVE_DIR/conf/hive-env.sh
    $HIVE_DIR/bin/schematool -initSchema -dbType derby
    echo -e "${GREEN}***** HIVE DONE! *****${NC}"
fi

## Graphalytics
sudo rm -rf $GRAPHALYTICS_CORE_DIR
sudo mkdir $GRAPHALYTICS_CORE_DIR
wget https://github.com/ldbc/ldbc_graphalytics/archive/v0.9.0.tar.gz -P $DOWNLOAD_DIR
tar -xzf $DOWNLOAD_DIR/v0.9.0.tar.gz -C $DOWNLOAD_DIR
sudo mv $DOWNLOAD_DIR/ldbc_graphalytics-0.9.0/* $GRAPHALYTICS_CORE_DIR
cd $GRAPHALYTICS_CORE_DIR
sudo mvn clean install
wget https://github.com/atlarge-research/graphalytics-platforms-graphx/archive/v0.1.tar.gz -P $DOWNLOAD_DIR
tar -xzf $DOWNLOAD_DIR/graphalytics-platforms-graphx-0.1.tar.gz -C $DOWNLOAD_DIR
cd $DOWNLOAD_DIR/graphalytics-platforms-graphx-0.1
# mvn package # crashes -> cannot find graphalytics core in central Maven repository


# Set hostname
echo $HOSTNAME > /etc/hostname
hostname $HOSTNAME
sed -i "s/.*127\.0\.0\.1.*/127\.0\.0\.1 localhost $HOSTNAME/" /etc/hosts
sed -i "s/.*127\.0\.1\.1.*/127\.0\.1\.1 $HOSTNAME/" /etc/hosts

# Restart SSH
sudo service ssh restart &>> /var/log/context.log

