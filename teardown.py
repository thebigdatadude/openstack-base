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
cluster_info='.ambari_cluster.info'
error_num=0

# Function for server teardown
def teardown_server(serverid):
	global error_num
	server = None
	try:	
		server = nova.servers.get(serverid)
	except:
		server = None
	if server is None:
		print 'Server: ' + serverid + ' not found please cleanup manually!'
		error_num += 1
	else:
		try:
			server.delete()
			print 'Deleted server: ' + server.id
		except:
			print 'Error on deleting server: ' + server.id + ' please cleanup manually!'
			error_num += 1

# Function for key-pair disposal
def teardown_keypair(keypairname):
	global error_num
	try:
		nova.keypairs.delete(keypairname)
		print 'Deleted keypair: ' + keypairname
	except:
		print 'Error on deleting keypair: ' + keypairname + ' please cleanup manually!'
		error_num += 1

# Read the .ambari_cluster.info file and stop all related services
with open(cluster_info) as f:
	content = f.readlines()

for line in content:
	if not line.startswith('#'):
		sline = line.strip();
		if len(sline) > 0:
			cmd = line.strip().split(': ')
			if cmd[0].strip() == 'keypair':
				teardown_keypair(cmd[1].strip())
			if cmd[0].strip() == 'server':
				teardown_server(cmd[1].strip())

# Finally delete the cluster_info file
if error_num == 0:
	os.remove(cluster_info)
else:
	print 'Errors occured file ' + cluster_info + ' was kept for reference'
