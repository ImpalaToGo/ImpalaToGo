#!/bin/bash

# script parameters are
# param 1 - number of servers to add.
# param 2 - access key 
# param 3 - secret key
# param 4 - DNS of the master node

COUNT=$1
ACCESS_KEY=$2
SECRET_KEY=$3
MASTER_NODE=$4
S3_BUCKET=$5
if [ -z $S3_BUCKET ];
then
	echo usage:
	echo "$0 <COUNT> <ACCESS_KEY> <SECRET_KEY> <MASTER_NODE_DNS> <S3_BUCKET>"
	exit 2
fi
#TODO: Create profile with keys
#TODO: Fixup logging
BATCH_ID=$(uuidgen)
. resize.config
take_lock
echo trying to start cluster $BATCH_ID
store_cluster_id $BATCH_ID
echo creating security group if required
. create_impala_security_group.sh
SECURITY_GROUP_IDS=$(get_or_create_security_group ${SECURITY_GROUP})
echo checking if requested AMI: $IMAGE_ID available
IMAGE_AVAILABLE=$($AWS_CMD describe-images --image-ids $IMAGE_ID|grep -o $IMAGE_ID)
if [ -z "$IMAGE_AVAILABLE" ];
then
	echo Image: $IMAGE_ID is not available
	echo please check configuration.
	exit 1
fi
echo requesting to start $COUNT instances of $INSTANCE_TYPE size with AMI: $IMAGE_ID
$AWS_CMD run-instances --image-id $IMAGE_ID --count $COUNT --instance-type $INSTANCE_TYPE --security-group-ids $SECURITY_GROUP_IDS --placement AvailabilityZone=$AVAILABILITY_ZONE --key-name $KEY_NAME --client-token $BATCH_ID --user-data "\'$USER_DATA\'" |tee -a $LOG

echo run-instances request sent, waiting for all instances to run

$AWS_CMD wait instance-running --filters Name=client-token,Values=$BATCH_ID |tee -a $LOG

echo all instances running querying instance details 
TMP_FILE=/tmp/${BATCH_ID}.info
$AWS_CMD describe-instances --filters Name=client-token,Values=$BATCH_ID >$TMP_FILE
echo getting DNS names
DNS_NAMES=$(grep $BATCH_ID <${TMP_FILE}|cut -f 14|tee ${CLUSTER_VAR_DIR}/hosts)

echo Adding ssh public-keys records to $SSH_KNOWN_HOSTS_FILE
echo -n " " >$SSH_KNOWN_HOSTS_FILE
#TODO: Add timeout
#TODO: There is still a possible bug when all hosts does not awaik togather,
#so when each host exposes more than one key, key count will be greater than required hosts but not enough to connect them all
while [ $(cat $SSH_KNOWN_HOSTS_FILE|wc -l) -lt $(echo $DNS_NAMES|wc -w) ]; do
	sleep 20
	echo trying to perform keyscan
	#clear known hosts before attempt
	echo -n  " " >$SSH_KNOWN_HOSTS_FILE
	for host in $DNS_NAMES; do
		ssh-keyscan -H $host >>$SSH_KNOWN_HOSTS_FILE
	done
done
echo Added $(cat $SSH_KNOWN_HOSTS_FILE|wc -l) keys for cluster

echo getting instance IDs
grep $BATCH_ID <${TMP_FILE}|cut -f8|tee ${CLUSTER_VAR_DIR}/instances

for host in $DNS_NAMES; do
	echo connecting to $host in background
	#TODO: You cannot simply run sudo as non-interractive user. Need to discover a way to run it
	ssh $SSH_PARAMS ec2-user@$host 'echo command running at `hostname` as user' & 
	ssh $SSH_PARAMS ec2-user@$host "sudo /home/ec2-user/attachToCluster.sh  $ACCESS_KEY $SECRET_KEY $MASTER_NODE $S3_BUCKET" &
done
echo waiting for all configuration commands to complete
wait

rm -f $TMP_FILE
echo All cluster nodes got configuration command. See master node $MASTER_NODE for details
release_lock
echo cluster $BATCH_ID is up and running.
