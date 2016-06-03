ssh-keyscan -t rsa,dsa ec2-52-33-233-47.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-35-104-88.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-26-177-150.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-32-88-7.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-27-119-236.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-26-59-27.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-26-6-129.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-26-234-160.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-11-23-58.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts
ssh-keyscan -t rsa,dsa ec2-52-35-119-75.us-west-2.compute.amazonaws.com 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts
mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts