#!/bin/sh
echo -e "Starting 9091 Server"
nohup sh {{ common['soft_link_base_path'] }}/kafka/bin/kafka-server-start.sh {{ common['soft_link_base_path'] }}/kafka/config/server.properties &
