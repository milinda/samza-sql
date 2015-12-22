import boto3


def get_instances():
    cat = {}
    cat['r3.2xlarge'] = []
    cat['r3.xlarge'] = []
    ec2 = boto3.resource('ec2')
    instances = ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    for instance in instances:
        cat[instance.instance_type].append({
            'dns_name': instance.public_dns_name,
            'private_ip': instance.private_ip_address
        })

    print '[kafkanodes]'
    print '{0} ansible_ssh_private_key_file=/Users/mpathira/Downloads/samzasql-eval-oregon-new.pem private_ip={1} kafka_broker_id={2} kafka_port=9092'.format(cat['r3.2xlarge'][0]['dns_name'], cat['r3.2xlarge'][0]['private_ip'], 1)
    print '{0} ansible_ssh_private_key_file=/Users/mpathira/Downloads/samzasql-eval-oregon-new.pem private_ip={1} kafka_broker_id={2} kafka_port=9092'.format(cat['r3.2xlarge'][1]['dns_name'], cat['r3.2xlarge'][1]['private_ip'], 2)
    print '{0} ansible_ssh_private_key_file=/Users/mpathira/Downloads/samzasql-eval-oregon-new.pem private_ip={1} kafka_broker_id={2} kafka_port=9092'.format(cat['r3.2xlarge'][2]['dns_name'], cat['r3.2xlarge'][2]['private_ip'], 3)
    print '[zookeepernodes]'
    print '{0} ansible_ssh_private_key_file=/Users/mpathira/Downloads/samzasql-eval-oregon-new.pem zookeeper_id={1} private_ip={2}'.format(cat['r3.xlarge'][0]['dns_name'], 1, cat['r3.xlarge'][0]['private_ip'])
    print '{0} ansible_ssh_private_key_file=/Users/mpathira/Downloads/samzasql-eval-oregon-new.pem zookeeper_id={1} private_ip={2}'.format(cat['r3.xlarge'][1]['dns_name'], 2, cat['r3.xlarge'][1]['private_ip'])
    print '{0} ansible_ssh_private_key_file=/Users/mpathira/Downloads/samzasql-eval-oregon-new.pem zookeeper_id={1} private_ip={2}'.format(cat['r3.xlarge'][2]['dns_name'], 3, cat['r3.xlarge'][2]['private_ip'])
    print '[rmnode]'
    print '{0} ansible_ssh_private_key_file=/Users/mpathira/Downloads/samzasql-eval-oregon-new.pem private_ip={1}'.format(cat['r3.xlarge'][3]['dns_name'], cat['r3.xlarge'][3]['private_ip'])
    print '[nmnodes]'
    print '{0} ansible_ssh_private_key_file=/Users/mpathira/Downloads/samzasql-eval-oregon-new.pem private_ip={1}'.format(cat['r3.2xlarge'][3]['dns_name'], cat['r3.2xlarge'][3]['private_ip'])
    print '{0} ansible_ssh_private_key_file=/Users/mpathira/Downloads/samzasql-eval-oregon-new.pem private_ip={1}'.format(cat['r3.2xlarge'][4]['dns_name'], cat['r3.2xlarge'][4]['private_ip'])
    print '{0} ansible_ssh_private_key_file=/Users/mpathira/Downloads/samzasql-eval-oregon-new.pem private_ip={1}'.format(cat['r3.2xlarge'][5]['dns_name'], cat['r3.2xlarge'][5]['private_ip'])
    print ''
    print '{0}:2181,{1}:2181,{2}:2181'.format(cat['r3.xlarge'][0]['dns_name'], cat['r3.xlarge'][1]['dns_name'], cat['r3.xlarge'][2]['dns_name'])
    print '{0}:9092,{1}:9092,{2}:9092'.format(cat['r3.2xlarge'][0]['dns_name'], cat['r3.2xlarge'][1]['dns_name'], cat['r3.2xlarge'][2]['dns_name'])

if __name__ == '__main__':
    get_instances()