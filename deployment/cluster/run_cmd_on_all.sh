#!/bin/bash
BATCH_ID=$1
shift
CMD="$1"
shift
. resize.config
for HOST in $(echo $CLUSTER_HOSTS);
do
	ssh $SSH_PARAMS ec2-user@$HOST "$CMD" & 
done
wait
