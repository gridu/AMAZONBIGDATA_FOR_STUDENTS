import subprocess
import time

import boto3
import paramiko
from paramiko import SSHClient
from scp import SCPClient


def run_shell(cmd):
    """
    Utility function to run shell command and print output to stdout
    :param cmd: shell command to run
    """
    try:
        output = subprocess.check_output(cmd.split(' '))
        for line in output.splitlines():
            print(line)
    except subprocess.CalledProcessError as e:
        print(e.output)


def create_cf_stack(stack_name, template_file_path, parameters):
    """
    :param stack_name:
    :param template_file_path: CF template location on local file system
    :param parameters: kv pairs that override CF template defaults/do not have defaults in CF template
    :type parameters: dict
    :return: id of the created stack
    """
    stack_parameters = []
    for key in parameters:
        stack_parameters.append({
            'ParameterKey': key,
            'ParameterValue': parameters[key]
        })
    with open(template_file_path) as template:
        cloud_formation = boto3.client('cloudformation')
        response = cloud_formation.create_stack(
            StackName=stack_name,
            TemplateBody=template.read(),
            Capabilities=['CAPABILITY_NAMED_IAM'],
            Parameters=stack_parameters
        )
        return response['StackId']


def wait_for_cf_stack_creation(stack_id):
    """
    Blocks until a CloudFormation stack with given id is created
    """
    cloud_formation = boto3.client('cloudformation')
    cloud_formation.get_waiter('stack_create_complete').wait(StackName=stack_id)


def get_cf_stack_output(stack_id):
    """
    :return: dict of outputs
    """
    cloud_formation = boto3.client('cloudformation')
    stack = cloud_formation.describe_stacks(StackName=stack_id)['Stacks'][0]
    outputs = stack['Outputs']

    result = {}
    for output in outputs:
        result[output['OutputKey']] = output['OutputValue']
    return result


def scp_to_ec2_user_home_dir(ip, pk, local_file):
    """
    Copies file on local file system to ec2-user's home dir, i.e. /home/ec2-user/
    :param ip: ip of ec2 machine
    :param pk: path to private key file on local file system
    :param local_file: file to copy
    :return:
    """
    ssh = get_ssh_client(ip, pk)
    scp = SCPClient(ssh.get_transport())
    scp.put(local_file)
    ssh.close()


def get_ssh_client(ip, pk):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        ip,
        username='ec2-user',
        key_filename=pk
    )
    return ssh


def execute_cmd_via_ssh(ip, pk, cmd):
    """
    Executes given command on machine at given IP address
    :param ip: IP of the machine to run command on
    :param pk: path to private key file on local file system
    :param cmd: shell command to execute
    :return:
    """
    ssh = get_ssh_client(ip, pk)
    ssh.exec_command(cmd)
    ssh.close()


def _wait_while_number_of_messages(queue, while_predicate):
    sqs = boto3.client('sqs')
    queue_url = sqs.get_queue_url(QueueName=queue)['QueueUrl']
    number_of_messages = None
    while number_of_messages is None or while_predicate(number_of_messages):
        time.sleep(10.0)
        response = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['ApproximateNumberOfMessages'])
        number_of_messages = int(response['Attributes']['ApproximateNumberOfMessages'])


def wait_for_messages_delivery_start(queue):
    """
    Blocks until number of messages in given queue becomes greater than 0
    """
    _wait_while_number_of_messages(queue, lambda num_of_messages: num_of_messages is 0)


def wait_for_messages_delivery_end(queue):
    """
    Blocks until number of messages in given queue is not 0
    """
    _wait_while_number_of_messages(queue, while_predicate=lambda num_of_messages: num_of_messages is not 0)


def wait_for_emr_steps_completion(emr_cluster_id, last_step_name):
    """
    Blocks until step with 'last_step_name' is not finished
    """
    emr = boto3.client('emr')
    steps = emr.list_steps(ClusterId=emr_cluster_id)['Steps']
    last_step = None
    for step in steps:
        if step['Name'] == last_step_name:
            last_step = step
            break
    emr.get_waiter('step_complete').wait(ClusterId=emr_cluster_id, StepId=last_step['Id'])


def create_athena_table(user, data_bucket):
    crawler_name = user
    dwh_stack_id = create_cf_stack(
        stack_name='{}-dwh'.format(user),
        template_file_path='aws/glue_cf_template.yaml',
        parameters={
            'DataBucket': data_bucket,
            'DataKeyPrefix': 'staging',
            'GlueDatabaseName': user,
            'CrawlerName': crawler_name
        }
    )
    wait_for_cf_stack_creation(dwh_stack_id)
    boto3.client('glue').start_crawler(Name=crawler_name)


if __name__ == '__main__':
    pass
