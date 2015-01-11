#!/bin/bash
GROUP_NAME=impala2go-slave

function is_group_exists(){
	local id=$(aws ec2  describe-security-groups --group-names $GROUP_NAME|grep $GROUP_NAME|grep -e "sg-[[:alnum:]]")
	echo $id
}
function set_tcp_port(){
	local PORT=$1
	aws ec2 authorize-security-group-ingress --group-name $GROUP_NAME --protocol tcp --port $PORT --cidr 0.0.0.0/0
}
function create_group(){
	aws ec2 create-security-group --group-name $GROUP_NAME --description "Security group for Impala2go slave node"
}
id=$(is_group_exists)
if [ ! -z "$id" ]; then
	echo Group $GROUP_NAME exists, ID=$id
else
	create_group
fi
#for metadata
set_tcp_port 80
#for SSH
set_tcp_port 22
set_tcp_port 22000
set_tcp_port 23000
set_tcp_port 25000
set_tcp_port 25010
set_tcp_port 25020
set_tcp_port 24000
set_tcp_port 26000
set_tcp_port 28000
set_tcp_port 15002
set_tcp_port 15000
set_tcp_port 15001
#TODO: implement ports query
echo please ignore errors above, they mean that your group is configured properly...
