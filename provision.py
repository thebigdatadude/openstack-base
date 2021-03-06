#!/usr/bin/env python

#
# Python script based on the python-openstack (v2) library and tools
# Automatically injects a CentOS 6.6 image, installs Puppet, creates servers, and rolls out Ambari

import time
import subprocess
import os.path
import socket

from novaclient.v2 import client as nvclient
from credentials import get_nova_creds

# Properly react to errors
class Error(Exception):
	pass

# Parse credentials (as provided through an openrc file)
creds = get_nova_creds()
nova = nvclient.Client(**creds)

# Default values
keypair_name = 'ambari-ssh'
centos_image = 'CentOS 6.6 64bit Puppet'
network_name = 'private'
ambari_public_ip = '140.78.92.57'
ambari_flavor = 'm1.medium'
worker_flavor = 'm1.xlarge'
ambari_security_group = 'ambari-ssh-http-8080'
vagrant_project_path = '/Users/matthias/Documents/workspace-bigdatadude/vagrant-base/'
number_of_workers = 10
domain_name='sandbox.thebigdatadude.com'
cluster_info='.ambari_cluster.info'

if os.path.isfile(cluster_info):
	raise Error('File ' + cluster_info + ' already exists. This means a cluster is already running. Deprovision the cluster first or if you are 100% sure you can just delete the file')

# Create a Log file which is later to be used to tear down the cluster
# Already append all info as comment
cluster_info_file = open(cluster_info, 'w')
cluster_info_file.write('# This is a cluster-info file that describes a provisioned HDP cluster\n')
cluster_info_file.write('# In the following all base configuration parameters are printed\n')
cluster_info_file.write('#\n')
cluster_info_file.write('# keypair-name: ' + keypair_name + '\n')
cluster_info_file.write('# centos_image: ' + centos_image + '\n')
cluster_info_file.write('# network_name: ' + network_name + '\n')
cluster_info_file.write('# ambari_public_ip: ' + ambari_public_ip + '\n')
cluster_info_file.write('# ambari_flavor: ' + ambari_flavor + '\n')
cluster_info_file.write('# worker_flavor: ' + worker_flavor + '\n')
cluster_info_file.write('# ambari_security_group: ' + ambari_security_group + '\n')
cluster_info_file.write('# vagrant_project_path: ' + vagrant_project_path + '\n')
cluster_info_file.write('#\n#\n#\n')

# Function that can be used to store information to the cluster_info file
def write_cluster_info(key, value):
	cluster_info_file.write(key + ': ' + value + '\n')

# Function that checks for existing SSH key-value pair
# if none was found just creates one
def ssh_keys(pvtkname):
	pubkname = pvtkname + '.pub'
	if os.path.isfile(pvtkname) and os.path.isfile(pubkname):
		print('Found existing keypair in ' + pvtkname + ', ' + pubkname)
	else:
		print('Generating new keypair in ' + pvtkname + ', ' + pubkname)
		subprocess.check_call(['ssh-keygen', '-t', 'rsa', '-N', '', '-f', pvtkname])
	return dict({'private' : pvtkname, 'public' : pubkname})

def find_floating_ip(wantedip, forceWanted=False):
	ips = nova.floating_ips.list()
	if wantedip is None:
		for cur_ip in ips:
			if cur_ip.instance_id is None:
				return cur_ip
	else:
		for cur_ip in ips:
			if cur_ip.ip == wantedip and cur_ip.instance_id is None:
				return cur_ip
			elif not forceWanted:
				return find_floating_ip(None)
	raise Error('Could not find a (or selected) floating IP')

# Function that creates a single server
def create_server(servername, imagename, networkname, flavorname, keypairname):
	image = nova.images.find(name=imagename)
	flavor = nova.flavors.find(name=flavorname)
	network = nova.networks.find(label=networkname)
	nics = [{'net-id': network.id}]
	server = nova.servers.create(name = servername,
		image = image.id,
		flavor = flavor.id,
		nics = nics,
		key_name = keypairname)
	return server

# Wait or server to switch to active state
def wait_for_server(server, network):
	while True:
		server = nova.servers.get(server.id)
		if server.status == 'ACTIVE':
			serverip = server.addresses[network][0]['addr']
			return serverip
		print('Waiting for server ' + server.name + ' to become ACTIVE')
		time.sleep(15)

# Check for service ports beeing available
def probe_port(ip, port):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	result = sock.connect_ex((ip, port))
	return result == 0

# Wait a minute for port becoming available
def wait_for_port(ip, port):
	tries = 5
	while tries > 0:
		if probe_port(ip, port):
			return
		else:
			tries -= 1
			print 'service ' + ip + ':' + str(port) + ' not availalbe waiting another 15sec'
			time.sleep(15)
	raise Error('Service ' + ip + ' at port ' + str(port) + ' did not become available in over a minute ... please check stack manually')

# Check for the security group we need
def find_security_group(sgname):
	sgs = nova.security_groups.list()
	for sg in sgs:
		if sg.name == sgname:
			return sg
	raise Error('Security group ' + sgname + ' not found please create the security group. We need port 22 (SSH) and 8080 (HTTP)')

# Execute a command on a remote machine
def execute_ssh(ip, private_key, command):
	try:
		subprocess.check_call(['ssh', '-q', '-t', '-i', private_key, '-o', 'StrictHostKeyChecking=no', 'centos@' + ip, command])
	except:
		print 'Non zero exit code on ssh command: ' + command + ' please check your cluster manually'

# Copy a folder to remote machine through rsync
def rsync_folder(ip, private_key, local_folder, remote_folder):
	subprocess.check_call(['rsync', '-azh', local_folder, '-e', 'ssh -i ' + private_key + ' -o StrictHostKeyChecking=no', 'centos@' + ip + ':' + remote_folder])

# Copy a single file to the remote machien through scp
def scp_file(ip, private_key, local_file, remote_folder):
	subprocess.check_call(['scp', '-i', private_key, '-o', 'StrictHostKeyChecking=no', local_file, 'centos@' + ip + ':' + remote_folder ])

# Instructs the remote machine to grow its partition
def grow_partition(ip, private_key):
	execute_ssh(ip, private_key, 'sudo yum -y -q install cloud-utils-growpart')
	execute_ssh(ip, private_key, 'sudo growpart /dev/vda 1')
	execute_ssh(ip, private_key, 'sudo reboot')

# SSH into Ambari server and bootstrap it
def bootstrap_ambari(ip, private_key):
	wait_for_port(ip, 22)
	tries = 5
	while tries > 0:
		try:
			# This first command has two functions
			# It probes for host availability and creates a tmp directory for rsync
			execute_ssh(ip, private_key, 'mkdir -p /tmp/provision')
			break
		except:
			tries -= 1
			time.sleep(15)
	if tries == 0:
		raise Error('Was not able to contact host ' + ip + ' within a minute')
	rsync_folder(ip, private_key, vagrant_project_path, '/tmp/provision')
	# Ensure compatibility with vagrant
	execute_ssh(ip, private_key, 'sudo mv /tmp/provision /vagrant')
	execute_ssh(ip, private_key, 'sudo chown -R root:root /vagrant')
	execute_ssh(ip, private_key, 'sudo puppet apply /vagrant/manifests/default.pp')

# Set the hostname
def set_hostname(ip, private_key, hostname):
	execute_ssh(ip, private_key, 'sudo sed -i "s/HOSTNAME=.*/HOSTNAME=' + hostname + '/g" /etc/sysconfig/network')
	execute_ssh(ip, private_key, 'sudo hostname ' + hostname)

# SSH key selection / creation
keypair = None
avail_keypairs = nova.keypairs.list()
for cur_keypair in avail_keypairs:
	if cur_keypair.id == keypair_name:
		keypair = cur_keypair
if keypair is None:
	kp = ssh_keys(keypair_name)
	f = open(kp['public'],'r')
	pubkey = f.readline()[:-1]
	keypair = nova.keypairs.create(keypair_name, pubkey)
write_cluster_info('keypair', keypair.id)

# Check if image exists
try:
	image = nova.images.find(name=centos_image)
except:
	raise Error('VM image "' + centos_image + '" not found. Please make sure you have an image called "' + centos_image + '" available on your OpenStack. The image can be downloaded from: http://cloud.centos.org/centos/6/images/CentOS-6-x86_64-GenericCloud.qcow2')

# Check if we can find a free floating-ip
ambari_public_ip = find_floating_ip(ambari_public_ip)

# Check if the necessary security group is available
ambari_security_group = find_security_group(ambari_security_group)

# Create the ambari server
ambari_server = create_server('ambari', centos_image, network_name, ambari_flavor, keypair_name)
ambari_private_ip = wait_for_server(ambari_server, network_name)
write_cluster_info('server', ambari_server.id)
# Assing the floating IP
ambari_server.add_floating_ip(ambari_public_ip)
ambari_server.add_security_group(ambari_security_group.id)
# SSH into ambari server
bootstrap_ambari(ambari_public_ip.ip, keypair_name)
set_hostname(ambari_public_ip.ip, keypair_name, 'ambari.' + domain_name)
grow_partition(ambari_public_ip.ip, keypair_name)

worker_private_ips = []
# Create worker nodes
tmp_public_ip = find_floating_ip(None)
for worker in range(1, number_of_workers+1):
	worker_name = 'node' + "{0:03d}".format(worker)
	print 'Provisioning worker: ' + worker_name 
	worker_server = create_server(worker_name, centos_image, network_name, worker_flavor, keypair_name)
	worker_ip = wait_for_server(worker_server, network_name)
	write_cluster_info('server', worker_server.id)
	worker_private_ips.append(worker_ip)
	worker_server.add_floating_ip(tmp_public_ip)
	worker_server.add_security_group(ambari_security_group.id)
	bootstrap_ambari(tmp_public_ip.ip, keypair_name)
	set_hostname(tmp_public_ip.ip, keypair_name, worker_name+ '.' + domain_name)
	grow_partition(tmp_public_ip.ip, keypair_name)
	worker_server.remove_security_group(ambari_security_group.id)
	worker_server.remove_floating_ip(tmp_public_ip)

# Create hosts file: Puppet just created a dummy hosts file suitable for local Vagrant deplyoments only
tmp_hosts_file_name = '.tmp.hosts'
thf = open(tmp_hosts_file_name, 'w')

thf.write('127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4\n::1         localhost localhost.localdomain localhost6 localhost6.localdomain6\n')
thf.write('\n')
thf.write('# the following entries are autogenerated by https://github.com/thebigdatadude/openstack-base\n')
thf.write(ambari_private_ip + '\tambari.' + domain_name + ' ambari\n')
for worker in range(1, number_of_workers+1):
	worker_name = 'node' + "{0:03d}".format(worker)
	thf.write(worker_private_ips[worker-1] + '\t' + worker_name + '.' + domain_name + ' ' + worker_name + '\n')
thf.close()

# upload hosts file to ambari server
print 'All machines provisioned updating hosts file on ambari server'
scp_file(ambari_public_ip.ip, keypair_name, tmp_hosts_file_name, '/tmp/generated-hosts-file')
execute_ssh(ambari_public_ip.ip, keypair_name, 'sudo mv /tmp/generated-hosts-file /etc/hosts')
print 'Instructing ambari server to copy hosts file to all workers'
time.sleep(25)
for ipw in worker_private_ips:
	execute_ssh(ambari_public_ip.ip, keypair_name, 'sudo scp -o StrictHostKeyChecking=no /etc/hosts root@' + ipw + ':/etc/hosts')

# Finally close the clusterinfo file
cluster_info_file.close()

# Print success message
print ''
print ''
print ''

print 'Ambari server was sucessfully provisioned you can access the dashboard at:'
print 'http://' + ambari_public_ip.ip + ':8080/'
print 'WARNING: Default passwords are still in place immideately change the password for the \'admin\' account.'
print 'User: admin, Password: admin'
print ''
print 'The following worker nodes can be used to provision your cluster'
for worker in range(1, number_of_workers+1):
	worker_name = 'node' + "{0:03d}".format(worker) + '.' + domain_name
	print worker_name

