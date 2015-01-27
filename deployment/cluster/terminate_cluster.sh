#!/bin/bash
BATCH_ID=$1

. resize.config
if [ -z $BATCH_ID ];
then
	echo Usage
	echo "   $0 <cluster id>"
	exit 1
fi
echo $($LOG_PREFIX) Terminating cluster $BATCH_ID|$LOG_APPEND
take_lock
function cleanup_cluster_residue(){
	rm -f $BATCH_ID
	rm -f $KNOWN_CLUSTERS_DIR/$BATCH_ID
	rm -rf $CLUSTER_VAR_DIR
	rm -f $TEMP_FILE
}

if [ -z "$AWS_CMD" ];
then
	AWS_CMD="aws ec2"
fi
set -x
echo $($LOG_PREFIX) Query instances for cluster $BATCH_ID|$LOG_APPEND
$AWS_CMD describe-instances $DRY_RUN --filters Name=client-token,Values=$BATCH_ID|$LOG_APPEND|tee $TEMP_FILE

INSTANCE_IDS=$(grep $BATCH_ID <$TEMP_FILE|cut -f8)
echo instance list: $INSTANCE_IDS|$LOG_APPEND
if [ -z "$INSTANCE_IDS" ];
then
	echo $($LOG_PREFIX) Cluster $BATCH_ID has no instances|$LOG_APPEND
	echo $($LOG_PREFIX) Either wrong ID provided, all instances were terminated by other provess or there is residue from other cluster-related scripts|$LOG_APPEND
	cleanup_cluster_residue|$LOG_APPEND
	release_lock
	exit 1
fi
for instance in $INSTANCE_IDS; do
	echo $($LOG_PREFIX) found instance: $instance|$LOG_APPEND
done
echo $($LOG_PREFIX) Terminating instances: $INSTANCE_IDS|$LOG_APPEND
$AWS_CMD terminate-instances --instance-ids $INSTANCE_IDS|$LOG_APPEND
echo $($LOG_PREFIX) terminate command sent, waiting termination|$LOG_APPEND
$AWS_CMD wait instance-terminated --instance-ids $INSTANCE_IDS|$LOG_APPEND
echo $($LOG_PREFIX) instances terminated, cleanup local resources|$LOG_APPEND
cleanup_cluster_residue|$LOG_APPEND
release_lock
echo $($LOG_PREFIX) Done|$LOG_APPEND
