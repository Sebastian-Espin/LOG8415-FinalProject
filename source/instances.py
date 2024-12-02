import boto3
import os
import paramiko
import time
from scripts import worker_script, update_manager_script, update_proxy_script, update_trusted_host_script, update_gatekeeper_script

class EC2Manager:
    def __init__(self):
        self.ec2 = boto3.resource('ec2', region_name='us-east-1')
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')
        self.key_pair_name = 'my_key_pair'
        self.userData_template = worker_script
        self.ssh = paramiko.SSHClient()

    def create_key_pair(self):
        try:
            key_pair = self.ec2_client.create_key_pair(KeyName=self.key_pair_name)
            private_key = key_pair['KeyMaterial']
            key_pair_file = f"{self.key_pair_name}.pem"

            with open(key_pair_file, "w") as file:
                file.write(private_key)

            print(f"Key pair '{self.key_pair_name}' created and saved as '{key_pair_file}'")
        except self.ec2_client.exceptions.ClientError as e:
            if 'InvalidKeyPair.Duplicate' in str(e):
                print(f"Key pair '{self.key_pair_name}' already exists.")
            else:
                raise
    
    def delete_key_pair(self):
        try:
            self.ec2_client.delete_key_pair(KeyName=self.key_pair_name)
            print(f"Deleted key pair '{self.key_pair_name}' from AWS.")

            key_pair_file = f"{self.key_pair_name}.pem"
            if os.path.exists(key_pair_file):
                os.remove(key_pair_file)
                print(f"Deleted local key pair file '{key_pair_file}'.")
        except self.ec2_client.exceptions.ClientError as e:
            print(f"Error deleting key pair: {e}")

    def wait_for_userdata_execution(self, instance_ip, key_file):
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        while True:
            try:
                # Attempt to connect to the instance
                self.ssh.connect(instance_ip, username="ubuntu", key_filename=key_file)
                print("SSH connection established.")
                while True:
                    try:
                        # Execute the command to check User Data execution in the instance output log
                        stdin, stdout, stderr = self.ssh.exec_command('tail -n 10 /var/log/cloud-init-output.log')
                        output = stdout.read().decode()
                        print(output)
                        if "finished at" in output:
                            print("User Data script execution completed.")
                            #TODO get outputs scripts and use them for plots
                            self.get_outputs(instance_ip)
                            #self.ssh.close()
                            return 
                        print("User Data script is still running. Checking again...")
                        time.sleep(10)

                    except paramiko.SSHException as ssh_err:
                        print(f"SSH command execution failed: {ssh_err}")
                        print("Waiting for the connection to come back...")
                        break  # Break to outer loop to try reconnecting
                #self.ssh.close()
                return

            except (paramiko.ssh_exception.NoValidConnectionsError, paramiko.ssh_exception.SSHException) as e:
                print("SSH connection failed. Retrying...")
                time.sleep(10)
            except Exception as e:
                print(f"An unexpected error occurred: {e}, Retrying...")
                time.sleep(20)

    def get_outputs(self, instance_ip):
        local_file_name = f'./source/data/sysbench_output_{instance_ip.replace(".", "_")}.txt'
        scp = paramiko.SFTPClient.from_transport(self.ssh.get_transport())
        scp.get('/home/ubuntu/sysbench_output.txt',local_file_name)
        scp.close()

    def launch_instances(self, security_group_id):
        workers = self.ec2.create_instances(
            ImageId='ami-0e86e20dae9224db8',
            MinCount=2,
            MaxCount=2,
            InstanceType='t2.micro',
            SecurityGroupIds=[security_group_id],
            KeyName=self.key_pair_name,
            UserData=self.userData_template,
            TagSpecifications=[
            {'ResourceType': 'instance', 'Tags': [{'Key': 'Role', 'Value': 'Worker'}]}
            ]
        )

        for worker in workers:
            worker.wait_until_running()
            worker.reload()
        manager_script = update_manager_script([worker.public_ip_address for worker in workers])
        manager = self.ec2.create_instances(
                    ImageId='ami-0e86e20dae9224db8',
                    MinCount=1,
                    MaxCount=1,
                    InstanceType='t2.micro',
                    SecurityGroupIds=[security_group_id],
                    KeyName=self.key_pair_name,
                    UserData=manager_script,
                    TagSpecifications=[
                    {'ResourceType': 'instance', 'Tags': [{'Key': 'Role', 'Value': 'Manager'}]}
                    ]
                )
        for instance in manager:
            instance.wait_until_running()
            instance.reload()
            
        print(manager[0].public_ip_address)
        print('Workers and Manager instances created, lauunching proxy and gatekeeper')
        self.launch_proxy_and_gatekeeper(security_group_id, manager[0].public_ip_address, [worker.public_ip_address for worker in workers])
        
        print('Proxy and gatekeeper launched, waiting for user data execution')
        instances = workers + manager
        for instance in instances:
            self.wait_for_userdata_execution(instance.public_ip_address, f"{self.key_pair_name}.pem")           


    def launch_proxy_and_gatekeeper(self, security_group_id, manager_ip, workers_ips):
        proxy_script = update_proxy_script(manager_ip, workers_ips)
        proxy_instance = self.ec2.create_instances(
            ImageId='ami-0e86e20dae9224db8',
            MinCount=1,
            MaxCount=1,
            InstanceType='t2.large',
            SecurityGroupIds=[security_group_id],
            KeyName=self.key_pair_name,
            UserData=proxy_script
        )
        proxy_instance[0].wait_until_running()
        proxy_instance[0].reload()
        print(f"Proxy instance created: {proxy_instance[0].public_ip_address}")

        trusted_host_script = update_trusted_host_script(proxy_instance[0].public_ip_address)
        trusted_host_instance = self.ec2.create_instances(
            ImageId='ami-0e86e20dae9224db8',
            MinCount=1,
            MaxCount=1,
            InstanceType='t2.large',
            SecurityGroupIds=[security_group_id],
            KeyName=self.key_pair_name,
            UserData=trusted_host_script
        )
        trusted_host_instance[0].wait_until_running()
        trusted_host_instance[0].reload()

        gatekeeper_script = update_gatekeeper_script(trusted_host_instance[0].public_ip_address)
        gatkeeper_instance = self.ec2.create_instances(
            ImageId='ami-0e86e20dae9224db8',
            MinCount=1,
            MaxCount=1,
            InstanceType='t2.large',
            SecurityGroupIds=[security_group_id],
            KeyName=self.key_pair_name,
            UserData=gatekeeper_script
        )
        gatkeeper_instance[0].wait_until_running()
        gatkeeper_instance[0].reload()
        print(f"Gatekeeper instance created: {gatkeeper_instance[0].public_ip_address}")

    
    def create_security_group(self, vpc_id):
        response = self.ec2.create_security_group(
            GroupName='my-security-group',
            Description='Security group for ALB and EC2 instances',
            VpcId=vpc_id
        )
        security_group_id = response.group_id

        self.ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 8000,
                    'ToPort': 8000,
                    'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                },
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 22,
                    'ToPort': 22,
                    'IpRanges': [{'CidrIp': "0.0.0.0/0"}]
                }
            ]
        )

        print(f"Created Security Group: {security_group_id}")
        return security_group_id

    def cleanup_resources(self):
        input(f"\nReady to terminate EC2 instances. Press Enter to proceed...")

        instances = self.ec2_client.describe_instances()
        instance_ids = [
            instance['InstanceId']
            for reservation in instances['Reservations']
            for instance in reservation['Instances']
            if instance['State']['Name'] != 'terminated'
        ]

        if instance_ids:
            self.ec2_client.terminate_instances(InstanceIds=instance_ids)
            self.ec2_client.get_waiter('instance_terminated').wait(InstanceIds=instance_ids)
            print(f"Terminated instances: {instance_ids}")

        response = self.ec2_client.describe_security_groups(
            Filters=[{'Name': 'group-name', 'Values': ['my-security-group']}]
        )

        if response['SecurityGroups']:
            security_group = response['SecurityGroups'][0]
            security_group_id = security_group['GroupId']

            try:
                self.ec2_client.delete_security_group(GroupId=security_group_id)
                print(f"Deleted Security Group")
            except self.ec2_client.exceptions.ClientError as e:
                print(f"Error deleting security group: {e}")
                
        self.delete_key_pair()

def main():
    ec2_manager = EC2Manager()
    ec2_manager.create_key_pair()
    vpc_id = ec2_manager.ec2_client.describe_vpcs()["Vpcs"][0]['VpcId']
    security_group = ec2_manager.create_security_group(vpc_id)
    ec2_manager.launch_instances(security_group)
    ec2_manager.cleanup_resources()

if __name__ == "__main__":
    main()
