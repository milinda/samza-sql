#!/bin/bash

MASTER_IP={{ hostvars[groups['rmnode'][0]]['private_ip'] }}
NUM_SLAVES={{ groups['nmnodes'] | length }}
SLAVE_IPS={% set comma = joiner(",") %}
                              {%- for host in groups['nmnodes'] -%}
                                {{ comma() }}{{ hostvars[host]['private_ip'] }}
                              {%- endfor %}

{% raw %}
set -e

#if [ -d "${HADOOP_HOME}" ]
#then
#  echo "WARNING: hadoop home : ${HADOOP_HOME} already exists. deleting."
#  rm -fr ${HADOOP_HOME}
#fi

#echo "creating new hadoop home: "
#mkdir -p ${HADOOP_HOME}

#echo "extracting hadoop archive ... "
#tar -zxvf ${HADOOP_ARCHIVE} -C ${HADOOP_HOME} --strip-components=1

echo "creating hadoop environment script... "
cat <<EOF > ${ENV_CONFIG}
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_MAPRED_HOME=${HADOOP_HOME}
export HADOOP_COMMON_HOME=${HADOOP_HOME}
export HADOOP_HDFS_HOME=${HADOOP_HOME}
export YARN_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export YARN_CONF_DIR=${HADOOP_HOME}/etc/hadoop
EOF

if [ -e ${CORE_DEFAULT} ]
then
  echo "core-default.xml already exists"
else
  echo "copying core-default.xml .. "
  cp ${HADOOP_HOME}/core-default.xml ${CORE_DEFAULT}
fi

echo "setting up core-site.xml .. "
cp ${HADOOP_HOME}/core-site.xml ${CORE_SITE}
sed -i -e "s;__MASTER_IP__;${MASTER_IP};g" -e "s;__HADOOP_HOME__;${HADOOP_HOME};g" ${CORE_SITE}

if [ -e ${YARN_DEFAULT} ]
then
  echo "yarn-default.xml already exists"
else
  echo "copying yarn-default.xml .. "
  cp ${HADOOP_HOME}/yarn-default.xml ${YARN_DEFAULT}
fi

echo "setting up container-executor.cfg .. "
cp ${HADOOP_HOME}/container-executor.cfg ${CONTAINER_EXECUTOR_CFG}

#echo "setting up yarn-site.xml .. "
#cp ${HADOOP_INSTALL_DIR}/yarn-site.xml ${YARN_SITE}
sed -i "s;__MASTER_IP__;${MASTER_IP};g" ${YARN_SITE}

echo "setting up mapred-site.xml .. "
cp ${HADOOP_HOME}/mapred-site.xml ${MAPRED_SITE}

echo "setting up slaves file .. "
echo "slaves list : ${SLAVE_IPS}"
echo ${SLAVE_IPS} | sed "s/,/\n/g" > ${SLAVES}

echo "fixing permissions ... "
chown -R hdadmin:hdadmin ${HADOOP_HOME}
chmod 755 ${HADOOP_HOME}/bin/container-executor 
mkdir -p ${HADOOP_HOME}/logs
chmod 777 ${HADOOP_HOME}/logs

echo "done installing hadoop environment"
{% endraw %}
