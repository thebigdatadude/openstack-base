#!/usr/bin/env python

#
# Python script based on the python-openstack (v2) library and tools
# Automatically injects a CentOS 6.6 image, installs Puppet, creates servers, and rolls out Ambari

import time
import subprocess
import os.path

from novaclient.v2 import client as nvclient
from credentials import get_nova_creds

# Parse credentials (as provided through an openrc file)
creds = get_nova_creds()
nova = nvclient.Client(**creds)

# Default values
keypair_name = 'ambari-ssh'
centos_image = 'CentOS 6.6 64bit Puppet'
network_name = 'private'
ambari_public_ip = '140.78.92.57'
ambari_flavor = 'm1.medium'
node_flavor = 'm1.medium'
ambari_security_group = 'ambari-ssh-http-8080'
vagrant_project_path = '/Users/matthias/Documents/workspace-bigdatadude/vagrant-base/'
cluster_info='.ambari_cluster.info'

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
cluster_info_file.write('# ambari_security_group: ' + ambari_security_group + '\n')
cluster_info_file.write('# vagrant_project_path: ' + vagrant_project_path + '\n')
cluster_info_file.write('#\n#\n#\n')

# Properly react to errors
class Error(Exception):
	pass

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

# Check for the security group we need
def find_security_group(sgname):
	sgs = nova.security_groups.list()
	for sg in sgs:
		if sg.name == sgname:
			return sg
	raise Error('Security group ' + sgname + ' not found please create the security group. We need port 22 (SSH) and 8080 (HTTP)')

# Execute a command on a remote machine
def execute_ssh(ip, private_key, command):
	subprocess.check_call(['ssh', '-q', '-t', '-i', private_key, '-o', 'StrictHostKeyChecking=no', 'centos@' + ip, command])

# Copy a folder to remote machine through rsync
def rsync_folder(ip, private_key, local_folder, remote_folder):
	subprocess.check_call(['rsync', '-azh', local_folder, '-e', 'ssh -i ' + private_key + ' -o StrictHostKeyChecking=no', 'centos@' + ip + ':' + remote_folder])


# SSH into Ambari server and bootstrap it
def bootstrap_ambari(ip, private_key):
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
# Assing the floating IP
ambari_server.add_floating_ip(ambari_public_ip)
ambari_server.add_security_group(ambari_security_group.id)
# SSH into ambari server
bootstrap_ambari(ambari_public_ip.ip, keypair_name)
write_cluster_info('server', ambari_server.id)

# Finally close the clusterinfo file
cluster_info_file.close()

# Print success message
print ''
print ''
print ''

print 'Ambari server was sucessfully provisioned you can access the dashboard at:'
print 'http://' + ambari_public_ip + ':8080/'
print 'WARNING: Default passwords are still in place immideately change the password for the \'admin\' account.'
print 'User: admin, Password: admin'
