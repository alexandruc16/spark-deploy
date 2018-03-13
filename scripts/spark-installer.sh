#!/bin/bash
NC='\033[0m' # no color
GREEN='\033[0;32m'
YELLOW='\033[1;33m'

sudo apt-get install -f
sudo apt-get update
sudo apt-get -y upgrade

## Git
echo -e "${YELLOW}Installing Development Tools${NC}"
apt install gcc make flex bison byacc git sbt maven
echo -e "${GREEN}*********** dev tools Done ************${NC}"

## Python
echo -e "${YELLOW}Installing Python and required libraries${NC}"
apt install python2.7 python-pip
pip2 install paramiko pyzmq psutil
echo -e "${GREEN}*********** Python Done ************${NC}"

DOWNLOAD_DIR=~/Downloads
ENVIRONMENT=/etc/environment
source $ENVIRONMENT


## JAVA
javac -help >/dev/null 2>&1
if [ $? == 0 ]; then
    echo -e "${GREEN}Java was found${NC}"
else
    echo -e "${YELLOW}Installing Oracle JDK v1.8${NC}"
    wget --no-check-certificate -c --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u161-b12/2f38c3b165be4555a1fa6e98c45e0808/jdk-8u161-linux-x64.tar.gz -P $DOWNLOAD_DIR
    tar -xzf $DOWNLOAD_DIR/jdk-8u161-linux-x64.tar.gz -C $DOWNLOAD_DIR
    mv $DOWNLOAD_DIR/jdk1.8.0_161 /usr/lib
    if [ -z "$JAVA_HOME" ]; then
        echo "export JAVA_HOME=/usr/lib/jdk1.8.0_161" >> $ENVIRONMENT
        echo "export PATH=$PATH:/usr/lib/jdk1.8.0_161/bin" >> $ENVIRONMENT
        source $ENVIRONMENT
    fi
    echo -e "${GREEN}***** JAVA DONE! *****${NC}"
fi

# Initialize directories
PROJ_DIR=/usr/local
HADOOP_DIR=$PROJ_DIR/hadoop
SCALA_DIR=$PROJ_DIR/scala
SPARK_DIR=$PROJ_DIR/spark
HIBENCH_DIR=/opt/hibench
SPARK_PERF_DIR=/opt/spark-perf
BIGBENCH_DIR=/opt/big-bench

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
    wget http://ftp.tudelft.nl/apache/hadoop/common/hadoop-2.7.5/hadoop-2.7.5.tar.gz -P $DOWNLOAD_DIR
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
    HADOOP_DIR=$HADOOP_COMMON_HOME
fi
yes | cp -a ./conf/hadoop/* $HADOOP_COMMON_HOME/etc/hadoop/


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
cp -a $SPARK_DIR/conf/spark-env.sh.template $SPARK_DIR/conf/spark-env.sh
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
touch $HIBENCH_DIR/conf/hadoop.conf
echo "hibench.hadoop.home   $HADOOP_DIR" >> $HIBENCH_DIR/conf/hadoop.conf
echo "hibench.hadoop.executable   $HADOOP_DIR/bin/hadoop" >> $HIBENCH_DIR/conf/hadoop.conf
echo "hibench.hadoop.configure.dir   $HADOOP_DIR/etc/hadoop" >> $HIBENCH_DIR/conf/hadoop.conf
echo "hibench.hadoop.release   apache" >> $HIBENCH_DIR/conf/hadoop.conf
echo "hibench.hdfs.master   hdfs://{{master_hostname}}:9000" >> $HIBENCH_DIR/conf/hadoop.conf
touch $HIBENCH_DIR/conf/spark.conf
echo "hibench.spark.home   $HADOOP_DIR" >> $HIBENCH_DIR/conf/spark.conf
echo "hibench.spark.master   spark://{{master_hostname}}:7077" >> $HIBENCH_DIR/conf/spark.conf
echo "hibench.spark.version   spark2.2" >> $HIBENCH_DIR/conf/spark.conf
echo -e "${GREEN}*********** HiBench Done ************${NC}"

cd /opt

## Bandwidth throttler
echo -e "${YELLOW}Installing bandwidth-throttler${NC}"
rm -rf /opt/bandwidth-throttler
git clone https://github.com/ovedanner/bandwidth-throttler.git
echo -e "${GREEN}*********** bandwidth-throttler Done ************${NC}"

## TPC-DS
echo -e "${YELLOW}Preparing TPC-DS${NC}"
rm -rf /opt/spark-tpc-ds-performance-test
git clone https://github.com/IBM/spark-tpc-ds-performance-test.git
sed -i 's@export SPARK_HOME=@export SPARK_HOME='"$SPARK_DIR"'@g' /opt/spark-tpc-ds-performance-test/bin/tpcdsenv.sh
echo -e "${GREEN}*********** bandwidth-throttler Done ************${NC}"

