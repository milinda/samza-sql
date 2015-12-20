set -e

export JAVA_HOME={{ java_home }}
export HADOOP_INSTALL_DIR={{ common['soft_link_base_path'] }}

#hadoop home is here irrespective of version
export HADOOP_HOME=${HADOOP_INSTALL_DIR}/hadoop
export MR_SCRIPT=${HADOOP_HOME}/test-pi-yarn.sh
export CORE_DEFAULT=${HADOOP_HOME}/etc/hadoop/core-default.xml
export CORE_SITE=${HADOOP_HOME}/etc/hadoop/core-site.xml
export YARN_DEFAULT=${HADOOP_HOME}/etc/hadoop/yarn-default.xml
export CONTAINER_EXECUTOR_CFG=${HADOOP_HOME}/etc/hadoop/container-executor.cfg
export YARN_SITE=${HADOOP_HOME}/etc/hadoop/yarn-site.xml
export MAPRED_SITE=${HADOOP_HOME}/etc/hadoop/mapred-site.xml
export SLAVES=${HADOOP_HOME}/etc/hadoop/slaves
export ENV_CONFIG=${HADOOP_HOME}/env.sh
