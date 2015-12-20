#!/bin/bash

source ${HADOOP_HOME}/env.sh

${HADOOP_HOME}/sbin/yarn-daemon.sh stop resourcemanager
${HADOOP_HOME}/sbin/yarn-daemon.sh start resourcemanager

${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh stop historyserver
${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh start historyserver

#${HADOOP_HOME}/sbin/hadoop-daemon.sh stop namenode
#${HADOOP_HOME}/bin/hadoop namenode -format -nonInteractive || true
#${HADOOP_HOME}/sbin/hadoop-daemon.sh start namenode
