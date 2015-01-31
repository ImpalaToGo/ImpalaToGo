#!/bin/bash
BATCH_ID=$1
CMD="$2"
. resize.config
for HOST in $(cat $CLUSTER_HOSTS);
do
	echo $HOST
	ssh $SSH_PARAMS ec2-user@$HOST "$CMD" & 
done
wait
