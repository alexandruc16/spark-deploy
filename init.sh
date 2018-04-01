#!/bin/bash

# -------------------------------------------------------------------------- #
# Copyright 2002-2011, OpenNebula Project Leads (OpenNebula.org)             #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License"); you may    #
# not use this file except in compliance with the License. You may obtain    #
# a copy of the License at                                                   #
#                                                                            #
# http://www.apache.org/licenses/LICENSE-2.0                                 #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS,          #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
#--------------------------------------------------------------------------- #

# Function that launches the given command and retries it until it fails.
function launch_and_retry {
	"$@"
	local status=$?
	while [ $status -ne 0 ]; do
		sleep 1
		echo "Could not launch $@, retrying" >> /home/$USERNAME/contextualization.log
		"$@"
		local status=$?
	done
}

if [ -f /mnt/context.sh ]
then
  . /mnt/context.sh
fi

echo $HOSTNAME > /etc/hostname
hostname $HOSTNAME
sed -i "s/.*127\.0\.1\.1.*/127\.0\.1\.1	$HOSTNAME/" /etc/hosts


if [ -f /mnt/$ROOT_PUBKEY ]; then
	mkdir -p /root/.ssh
	cat /mnt/$ROOT_PUBKEY >> /root/.ssh/authorized_keys
	 chmod -R 600 /root/.ssh/
	chmod 600 /root/.ssh/authorized_keys
	chmod 700 /root/.ssh
fi

if [ -n "$USERNAME" ]; then
	useradd -s /bin/bash -m $USERNAME
	echo "$USERNAME:1234" | chpasswd
	sed -i "s/.*PasswordAuthentication.*/PasswordAuthentication yes/" /etc/ssh/sshd_config
	sed -i "s/.*MaxStartups.*/Maxstartups 10000/" /etc/ssh/sshd_config
	sed -i "s/.*StrictHostKeyChecking.*/StrictHostKeyChecking no/" /etc/ssh/ssh_config
	service ssh restart
	if [ -f /mnt/$USER_PUBKEY ]; then
		mkdir -p /home/$USERNAME/.ssh/
		cat /mnt/$USER_PUBKEY >> /home/$USERNAME/.ssh/authorized_keys
		chown -R $USERNAME:$USERNAME /home/$USERNAME/.ssh
		# chmod -R 600 /home/$USERNAME/.ssh/authorized_keys
		chmod 600 /home/$USERNAME/.ssh/authorized_keys

		# add sudo to look around on system:
		echo "$USERNAME ALL=(ALL) NOPASSWD: /bin/bash *" >>/etc/sudoers
		# check:
		cp /etc/sudoers /etc/sudoers.copy
		chmod 644 /etc/sudoers.copy
	fi
fi

touch /home/$USERNAME/contextualization.log
echo "Hostname is : $HOSTNAME" >> /home/$USERNAME/contextualization.log
echo "Netmask is : $NETMASK" >> /home/$USERNAME/contextualization.log
echo "Gateway is : $GATEWAY" >> /home/$USERNAME/contextualization.log
echo "DNS is : $DNS" >> /home/$USERNAME/contextualization.log

# Not sure if setting the DNS this way does anything..
if [ -n "$DNS" ]; then
        echo "Setting DNS server to $DNS" >>/var/log/context.log
        echo "nameserver $DNS" >/etc/resolv.conf
fi

# Bringing the interface down and up seems to set the correct IP address.
# Afterwards, we can set the correct netmask.
if [ -n "$NETMASK" ]; then
	ifconfig eth0 $IP_PUBLIC
	echo "SETTING NETMASK: $NETMASK\n" >> /var/log/netmask.log
	ifconfig eth0 netmask $NETMASK &>> /var/log/netmask.log
else
	echo "NETMASK DOES NOT EXIST\n" >> /var/log/netmask.log
fi

# Add the gateway.
if [ -n "$GATEWAY" ]; then
	echo "SETTING GATEWAY: $GATEWAY\n" >> /var/log/netmask.log
	route add default gw $GATEWAY
else
	echo "GATEWAY DOES NOT EXIST\n" >> /var/log/netmask.log
fi

# Make sure the repositories are up to date.
#mkdir -p /local/$USERNAME/logs
#if [ -d "/home/$USERNAME/bandwidth-throttler" ]; then
#	echo "BANDWIDTH THROTTLER EXISTS" >> /home/$USERNAME/contextualization.log
#else
#	launch_and_retry git clone https://github.com/ovedanner/bandwidth-throttler.git  /home/$USERNAME/bandwidth-throttler &>> /home/$USERNAME/contextualization.log

	# Put the bandwidth throttle binary in the right location.
#	cp /home/$USERNAME/bandwidth-throttler/shape_traffic.sh /usr/bin/shape_traffic

	# Run the bandwidth throttling server and bandwidth monitoring tool.
#	cd /home/$USERNAME/bandwidth-throttler
#	nohup python monitor_bandwidth.py eth0 /local/$USERNAME/proc_bandwidth.out /local/$USERNAME/proc_bandwidth.in proc 2>&1 > /dev/null &
#	nohup python monitor_bandwidth.py eth0 /local/$USERNAME/psutil_bandwidth.out /local/$USERNAME/psutil_bandwidth.in psutil 2>&1 > /dev/null &
#	nohup python shape_traffic_server.py --port 5555 2>&1 > /local/$USERNAME/logs/shape_traffic_server.log &

#    echo "finished with the bw throttler" >> /home/$USERNAME/contextualization.log

#fi


# Copy the given .bashrc file to the right location.
#if [ -f /mnt/.bashrc ]
#then
#  cp /mnt/.bashrc /home/$USERNAME
#  chown $USERNAME:$USERNAME /home/$USERNAME/.bashrc
#fi

# Make sure the .bashrc file is also loaded for non-interactive shells.
#echo "BASH_ENV=~/.bashrc" >> /home/$USERNAME/.ssh/environment
#chown $USERNAME:$USERNAME /home/$USERNAME/.ssh/environment

# Make sure the user owns the files.
#chown -R $USERNAME:$USERNAME /home/$USERNAME

