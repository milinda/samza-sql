#!/usr/bin/env bash
ssh-keyscan -t rsa,dsa ec2-52-27-95-243.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-89-171-68.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-89-202-196.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-25-134-249.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-89-36-253.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-88-142-32.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
