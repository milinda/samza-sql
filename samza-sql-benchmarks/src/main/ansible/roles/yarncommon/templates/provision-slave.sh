#!/bin/bash

set -e


MASTER_IP={{ hostvars[groups['rmnode'][0]]['private_ip'] }}
NUM_SLAVES={{ groups['nmnodes'] | length }}
SLAVE_IPS={% set comma = joiner(",") %}
                              {%- for host in groups['nmnodes'] -%}
                                {{ comma() }}{{ hostvars[host]['private_ip'] }}
                              {%- endfor %}

SCRIPT_DIR={{ common['soft_link_base_path'] }}/hadoop/pbin

{% raw %}
MASTER_NAME=hadoop-master
SLAVE_NAMES=($(eval echo hadoop-slave-{1..${NUM_SLAVES}}))

# Setup hosts file to support ping by hostname to master
#if [ ! "$(cat /etc/hosts | grep $MASTER_NAME)" ]; then
#  echo "Adding $MASTER_NAME to hosts file"
#  echo "$MASTER_IP $MASTER_NAME" >> /etc/hosts
#fi

# Setup hosts file to support ping by hostname to each slave in the cluster
#slave_ip_array=(${SLAVE_IPS//,/ })
#for (( i=0; i<${#SLAVE_NAMES[@]}; i++)); do
#  slave=${SLAVE_NAMES[$i]}
#  ip=${slave_ip_array[$i]}
#  if [ ! "$(cat /etc/hosts | grep $slave)" ]; then
#    echo "Adding $slave to hosts file"
#    echo "$ip $slave" >> /etc/hosts
#  else
#    host_entry=$(cat /etc/hosts | grep $slave)
#    ip_in_file=$(echo $host_entry | awk '{print $1}')
#    echo "existing host entry is \"$host_entry\""
#    echo "ip is \"$ip_in_file\""
#    if [ "$ip_in_file" == "127.0.0.1" ]; then
#      echo "$slave has a 127.0.0.1 entry - fixing." 
#      sed -i "s/127\.0\.0\.1.*/127.0.0.1 localhost/g" /etc/hosts
#      echo "Adding $slave to hosts file"
#      echo "$ip $slave" >> /etc/hosts
#    fi
#  fi
#done

echo "Installing hadoop ..."
pushd $SCRIPT_DIR
source ./hadoop-config.sh
./provision-hadoop.sh $MASTER_IP $NUM_SLAVES $SLAVE_IPS
./restart-hadoop-slave-daemons.sh
popd
{% endraw %}
