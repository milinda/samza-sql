import boto3


def get_instances():
    ec2 = boto3.resource('ec2')
    instances = ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    for instance in instances:
        print 'ssh-keyscan -t rsa,dsa {0} 2>&1 | sort -u - ~/.ssh/known_hosts > ~/.ssh/tmp_hosts '.format(instance.public_dns_name)
        print 'mv ~/.ssh/tmp_hosts ~/.ssh/known_hosts'


if __name__ == '__main__':
    get_instances()