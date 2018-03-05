#!/bin/bash 
sudo apt-get update > /dev/null 
sudo apt-get -y upgrade > /dev/null

## JAVA
command -v javac>/dev/null 2>&1 || { echo >&2 "I require java but it's not \
    installed. Installing java"; wget --no-check-certificate -c --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u161-b12/2f38c3b165be4555a1fa6e98c45e0808/jdk-8u161-linux-x64.tar.gz -P $DOWNLOAD_DIR;
    tar -xzf $DOWNLOAD_DIR/jdk-8u161-linux-x64.tar.gz; sudo mv jdk1.8.0_161 /usr/lib }
# if java_home is not set
if [ -z "$JAVA_HOME" ]; then
    echo "export JAVA_HOME=/usr/lib/jdk1.8.0_161" >> ~/.bashrc
	source ~/.bashrc
fi

# Initialize directories
pwd=$PWD
SRC_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SRC_DIR
cd ..
cd $pwd
DOWNLOAD_DIR=~/Downloads
PROJ_DIR=/usr/local
CONF_FILES_DIR=$PROJ_DIR/config-files
HADOOP_DIR=$PROJ_DIR/hadoop
SCALA_DIR=$PROJ_DIR/scala
SPARK_DIR=$PROJ_DIR/spark
# if Downloads folder does not exist, create it
if [ ! -d $DOWNLOAD_DIR ]; then
    mkdir $DOWNLOAD_DIR
fi
cd $DOWNLOAD_DIR
if [ ! -d $PROJ_DIR ]; then
    mkdir $PROJ_DIR
fi


## HADOOP
hadoop -h 2>&1>/dev/null
if [ $? == 0 ]; then
    echo 'Hadoop was found'
else
    wget http://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-2.7.5/hadoop-2.7.5.tar.gz -P $DOWNLOAD_DIR
    tar -xzf $DOWNLOAD_DIR/hadoop-2.7.5.tar.gz
    mv hadoop-2.7.5 $HADOOP_DIR
    if [ -z "$HADOOP_PREFIX" ]; then
        echo "export HADOOP_PREFIX=$HADOOP_DIR" >> ~/.bashrc
        echo "export PATH=$PATH:$HADOOP_DIR/bin" >> ~/.bashrc
        echo "export PATH=$PATH:$HADOOP_DIR/sbin" >> ~/.bashrc
        echo "export HADOOP_MAPRED_HOME=$HADOOP_DIR" >> ~/.bashrc
        echo "export HADOOP_COMMON_HOME=$HADOOP_DIR" >> ~/.bashrc
        echo "export HADOOP_HDFS_HOME=$HADOOP_DIR" >> ~/.bashrc
        echo "export YARN_HOME=$HADOOP_DIR" >> ~/.bashrc
        source ~/.bashrc
    fi

    #create directory for hadoop to store files
    mkdir -p /home/$USER/hadoop_data
    mkdir -p /home/$USER/hadoop_data/hdfs/namenode
    mkdir -p /home/$USER/hadoop_data/hdfs/datanode
    # copy configuration files for hadoop 
    cp $CONF_FILES_DIR/core-site.xml $HADOOP_DIR/conf
    cp $CONF_FILES_DIR/mapred-site.xml $HADOOP_DIR/conf
    cp $CONF_FILES_DIR/hdfs-site.xml $HADOOP_DIR/conf
#    mv $HADOOP_DIR/conf/hadoop-env.sh.template $HADOOP_DIR/conf/hadoop-env.sh
    #set JAVA_HOME in hadoop config file
    echo "export JAVA_HOME=/usr/lib/jdk1.8.0_161" >> $HADOOP_DIR/conf/hadoop-env.sh
    # Format the name node
    chmod -R 777 $HADOOP_DIR
    hadoop namenode -format

    echo "***** HADOOP DONE! *****"

fi

## SCALA
scalac -help 2>&1>/dev/null
if [ $? == 0  ]; then
    echo 'Scala was found'
else
    # download the src for hadoop, scala and spark
    wget https://github.com/scala/scala/archive/v2.11.12.tar.gz -P $DOWNLOAD_DIR
    #Extract hadoop and Scala
    tar -zxf scala-2.11.12.tgz
    mv scala-2.11.12 $SCALA_DIR
    chmod -R 777 $SCALA_DIR
    echo "***** Now Copying Scala *****"
    echo "export SCALA_HOME=$SCALA_DIR" >> ~/.bashrc
    echo "export PATH=$PATH:$SCALA_DIR/bin" >> ~/.bashrc
fi

## SPARK
#wget http://www.spark-project.org/download/spark-0.7.3-sources.tgz -P $DOWNLOAD_DIR
wget http://apache.mirror.triple-it.nl/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz -P $DOWNLOAD_DIR
tar -xzf $DOWNLOAD_DIR/spark-2.2.1-bin-hadoop2.7.tgz
# Now build Spark
cp -R $DOWNLOAD_DIR/spark-2.2.1-bin-hadoop2.7 $SPARK_DIR

mv $SPARK_DIR/conf/spark-env.sh.template $SPARK_DIR/conf/spark-env.sh
echo "export SCALA_HOME=$SCALA_DIR" >> $SPARK_DIR/conf/spark-env.sh

#set hadoop version in spark build
sed -i 's|1.0.4|2.2.0|' $SPARK_DIR/project/SparkBuild.scala
echo "SPARK_YARN=true" | tee -a $SPARK_DIR/project/SparkBuild.scala
cd $SPARK_DIR
sbt/sbt assembly
echo "*********** Spark Done ************"

